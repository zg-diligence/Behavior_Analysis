"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author:Gang Zhang
  Date:2018.02.01

  Function:
    Classify all unclassified programs by extracting prefixes:
        1.extracting common prefix among all unclassified programs
          these prefixes may be name of prorams from <电视剧, 少儿, 纪实, 科教, etc>

        2.classify the prefixes by gold programs

        2.crawl relative programs info from http://www.tvmao.com

        3.classify the rest prefixes by info crawled from above site

        4.category of relative programs detemined by their common prefixes
"""

import os, re, json
import time, codecs
from random import randint, choice
from multiprocessing import Pool, Manager

import requests
from bs4 import BeautifulSoup
from urllib.parse import quote

from proxypool import ProxyPool
from program_first_classifyer import Classifyer
from program_third_classifyer import DistanceClassifyer
from basic_category import categories as all_categories

DEBUG = True
TMP_PATH = os.getcwd() + '/tmp_result'
SCRAPY_PATH = TMP_PATH + '/scrapy_programs'
RESULT_PATH = TMP_PATH + '/classified_result'


class Scrapyer(object):
    """
    Function:
        1.search programs from http://www.tvmao.com and collect relative programs info
        2.crawl detail info of programs from http://www.tvmao.com to help classify programs
    """

    def __init__(self):
        self.retry_count = 3
        self.empty_count = 0
        self.pre_empty_flag = False

        self.enabled_programs = []
        self.unabled_programs = []
        self.collected_programs = []

        self.proxypool = ProxyPool()
        self.proxy = self.proxypool.get_proxy()
        self.headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/'
                        '537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}

    def change_proxy(self):
        """
        change current proxy
        :return:
        """

        self.proxypool.delete_proxy(self.proxy)
        self.proxy = self.proxypool.get_proxy()

    def check_empty(self, num, source_programs, lock):
        """
        check whether if the current proxy is dead
        if the res '[]' occurs over 5 times consecutively
        :param num: number of current columns
        :param source_programs: programs need to crawl
        :param lock: lock to access the source_programs
        :return:
        """

        if num == 0:
            if self.pre_empty_flag:
                self.empty_count += 1
                if self.empty_count >= 5:
                    for i in range(5, 0, -1):
                        program = self.unabled_programs[i]
                        if empty_times[program] < 2:
                            self.unabled_programs.pop(i)
                            with lock:
                                source_programs.put(program)
                                empty_times[program] += 1
                    self.change_proxy()
                    self.empty_count = 0
            else:
                self.pre_empty_flag = True
                self.empty_count = 1
        elif self.pre_empty_flag:
            self.pre_empty_flag = False
            self.empty_count = 0

    def collect_programs(self, page_uls, page_columns):
        """
        parse programs from the crawed result by columns
        :param page_uls: all uls in the result
        :param page_columns: all categories in the result
        :return:
        """

        prefix = 'http://www.tvmao.com'

        programs = []
        for column, uls in zip(page_columns, page_uls):
            lis = uls.find_all('li', class_='mr10')
            if len(lis) == 0: continue
            if re.search('^(电视剧|电影)', column):
                href_names = [(prefix + li.p.a['href'], li.p.a.get_text()) for li in lis]
            elif re.search('^(综艺|明星|赛事)', column):
                href_names = [(prefix + li.a['href'], li.a['title']) for li in lis]
            else:
                continue
            programs.append(href_names)
        return dict(zip(page_columns, programs))

    def crawl_relative_program(self, program, source_programs, lock):
        """
        crawl relative programs info from http://www.tvmao.com
        :param program:
        :param source_programs: all programs need to crawl
        :param lock: lock to access the source_programs
        :return:
        """

        url = 'http://www.tvmao.com/query.jsp?keys=%s&ed=' % quote(program) + \
              'bOWkp%2BeZveWkq%2BWmh%2BS4iua8lOazoeayq%2BS5i%2BWQu28%3D'

        # crawl the website
        bsObj = None
        self.retry_count = 3
        while self.retry_count > 0:
            try:
                content = requests.get(url, proxies={'http': self.proxy}, headers=self.headers, timeout=2)
                bsObj = BeautifulSoup(content.text, 'html.parser')
                break
            except:
                self.retry_count -= 1
                if self.retry_count <= 0:
                    if DEBUG: print("waiting...")
                    self.change_proxy()
                    self.retry_count = 3

        # parse infomation
        try:
            page_content = bsObj.find_all('div', class_='page-content')[0]
            page_columns = [item.a.get_text() for item in page_content.dl.find_all('dd')]
            page_columns = [column for column in page_columns if not re.search('^(播出时间|电视频道)', column)]
            page_content_uls = page_content.div.find_all('ul', class_=re.compile('^.+qtable$'), recursive=False)
            if len(page_columns) == 0:
                self.unabled_programs.append(program)
            else:
                self.enabled_programs.append(program)
                column_programs = self.collect_programs(page_content_uls, page_columns)
                return {program: column_programs}

            # check whether if the current proxy was dead
            self.check_empty(len(page_columns), source_programs, lock)
        except:
            with lock:
                source_programs.put(program)
            self.change_proxy()
            return None

    def run_crawl_relative_programs(self, source_programs, lock, limit=False):
        """
        single process
        :param source_programs: all programs need to crawl
        :param lock: lock to access the source_programs
        :param limit: if size of source_programs has little change, end process when limit is true
        :return: collected programs info, enabled programs, unabled programs
        """

        collected_programs = []
        # count, pre = 0, source_programs.qsize()
        while True:
            try:
                with lock:
                    program = source_programs.get_nowait()
                if DEBUG: print(source_programs.qsize())

                if  source_programs.qsize() < 1500:
                    return collected_programs, self.enabled_programs, self.unabled_programs
                # count += 1
                # if count % 50 == 0 and limit:
                #     if pre - source_programs.qsize() < 0:
                #         return collected_programs, self.enabled_programs, self.unabled_programs
                # pre = source_programs.qsize()

                result = self.crawl_relative_program(program, source_programs, lock)
                if result: collected_programs.append(result)
                time.sleep(randint(0, 1))
            except:
                return collected_programs, self.enabled_programs, self.unabled_programs

    def category_classify(self, category):
        """
        classify by the category from xingchen
        :param category: program intro or program category from xingchen
        :return:
        """

        if re.search('军旅', category): return '军事'
        if re.search('纪录片', category): return '纪实'
        if re.search('动漫', category): return '少儿'
        if re.search('戏剧', category): return '戏曲'
        if re.search('真人秀', category): return '综艺'
        res = re.search('|'.join(all_categories), category)
        if res: return res.group()
        return None

    def intro_classify(self, intro):
        """
        classify the category 'living' into more accurate category
        :param intro: introduction of the realtive program in xingchen
        :return:
        """

        if re.search('军旅', intro): return '军事'
        if re.search('纪录片', intro): return '纪实'
        if re.search('动漫', intro): return '少儿'
        if re.search('戏剧', intro): return '戏曲'
        if re.search('真人秀', intro): return '综艺'
        res = re.search('|'.join(all_categories), intro)
        if res: return res.group()
        return "生活"

    def search_to_classify_program(self, href):
        """
        classify programs by crawling more detail info from xingchen
        :param href: link of the relative program in xingchen
        :return:
        """

        # crawling the website
        bsObj = None
        self.retry_count = 3
        while self.retry_count > 0:
            try:
                content = requests.get(href, proxies={'http': self.proxy}, headers=self.headers, timeout=2)
                if content.status_code != 200:
                    self.retry_count -= 1
                    if self.retry_count <= 0:
                        if DEBUG: print("waiting...")
                        self.change_proxy()
                        self.retry_count = 3
                    continue
                bsObj = BeautifulSoup(content.text, 'html.parser')
                break
            except:
                self.retry_count -= 1
                if self.retry_count <= 0:
                    if DEBUG: print("waiting...")
                    self.change_proxy()
                    self.retry_count = 3

        # classify the program by detail info from website
        try:
            if re.search('tvcolumn', href):
                res_1 = bsObj.find_all('td', class_='gray pl15')
                if res_1:
                    category = res_1[0].findNext('td').get_text()
                    if category != "生活":
                        category = self.category_classify(category)
                        return category if category else '综艺'
                    div = bsObj.find_all('div', class_='clear more_c')[0]
                    intro = '; '.join([p.get_text() for p in div.find_all('p')])
                    return self.intro_classify(intro)
                else:
                    return '综艺'
            elif re.search('drama', href):
                mark = bsObj.find(text='类别：')
                td = mark.parent.findNext('td')
                category = ' '.join([a.get_text() for a in td.find_all('a', recursive=False)])
                category = self.category_classify(category)
                return category if category else '电视剧'
        except:
            if DEBUG: print("fuck", href)
            return choice(['综艺', '电视剧'])

    def run_search_to_classify_programs(self, source_items, lock):
        """
        single process
        :param source_items: all programs need to crawl more detail info
        :param lock: lock to access source_items
        :return:
        """

        program_cateogry = []
        while True:
            try:
                with lock:
                    item = source_items.get_nowait()
                if DEBUG: print(source_items.qsize())
                category = self.search_to_classify_program(item[2])
                program_cateogry.append((item[0], category))
                time.sleep(randint(0, 1))
            except:
                return program_cateogry


class PrefixClassifyer(object):
    """
    Function:
        1.search common prefix
        2.classify prefixes by gold programs
        3.search relative programs info of prefixes from http://www.tvmao.com
        3.classify prefixes by crawl relative programs info above
    """

    def __init__(self):
        self.scrapyer = Scrapyer()

    def get_common_prefix(self, pre_str, cur_str, N=4):
        """
        find common prefix between pre_str and cur_str
        minimal length of the prefix is N
        :param pre_str:
        :param cur_str:
        :param N:
        :return:
        """

        if len(pre_str) < N or len(cur_str) < N:
            return None

        if cur_str[:N] == pre_str[:N]:
            prefix = cur_str[:N]
            index = N
            max_index = min(len(cur_str), len(pre_str))
            while index < max_index:
                if cur_str[index] == pre_str[index]:
                    index, prefix = index + 1, cur_str[:index + 1]
                else:
                    break
            return prefix
        return None

    def get_best_prefix(self, programs, prefix, pre, cur, min_length):
        """
        find best prefix among continuously items
        :param programs: all continuous programs
        :param prefix: current prefix
        :param pre: pos of the pre item
        :param cur: pos of the current item
        :param min_length: minimal length of the best prefix
        :return: if the best prefix is made up with pure number, return None, pre, cur, []
                else return best_prefix, pre, cur, continuous_programs
        """

        start_pos = pre
        pre, cur = cur, cur + 1
        max_length = len(programs)
        while cur < max_length:
            pre_str, cur_str = programs[pre], programs[cur]
            res = self.get_common_prefix(pre_str, cur_str, min_length)
            if res is None:
                if not re.match('^\d+$', prefix[:4]):
                    return prefix, pre, cur, programs[start_pos: cur]
                return None, pre, cur, []
            else:
                prefix = res if len(res) < len(prefix) else prefix
                pre, cur = cur, cur + 1

    def search_common_prefix(self):
        """
        search all possible prefixes among all programs
        :return:prefixes and relative programs
        """

        with codecs.open(RESULT_PATH + '/reclassify_programs_1.txt', 'r') as fr:
            programs = [line.strip() for line in fr.readlines()]

        prefixes, extracted_programs = [], []

        # search common prefix
        pre, cur = 0, 1
        N, max_length = 4, len(programs)
        while cur < max_length:
            pre_str, cur_str = programs[pre], programs[cur]
            prefix = self.get_common_prefix(pre_str, cur_str)
            if prefix:
                res_4 = self.get_best_prefix(programs, prefix, pre, cur, min_length=N)
                res_3 = self.get_best_prefix(programs, prefix, pre, cur, min_length=N-1)
                res = res_4
                if len(res_3[3]) - len(res_4[3]) >= 5:
                    res = res_3
                if len(res[3]) >= 4:
                    if not re.search('^(CCTV|unit)|的$', prefix):
                        pre, cur = res[1], res[2]
                        prefixes.append(res[0])
                        extracted_programs.append(res[3])
            pre, cur = cur, cur + 1

        # normalize the prefixes
        handled_prefixes = []
        for prefix in prefixes:
            res = prefix
            if re.search('\D\D(\d{1,2}|之)$', prefix):
                res = re.sub('(\d{1,2}|之)$', '', prefix)
            handled_prefixes.append(res)
        prefixes = handled_prefixes

        # write info file
        if DEBUG: print(sum([len(programs) for programs in extracted_programs]))
        with codecs.open(TMP_PATH + '/prefix_programs.txt', 'w') as fw:
            fw.write('\n'.join(sorted(set(prefixes))))
        with codecs.open(TMP_PATH + '/prefix_lists.txt', 'w') as fw:
            for prefix, programs in zip(prefixes, extracted_programs):
                fw.write('The prefix:' + prefix + '\n')
                fw.write('\n'.join(programs) + '\n\n')

        return list(zip(prefixes, extracted_programs))

    def read_gold_programs(self):
        """
        read all crwaled programs from file
        :return:
        """

        all_programs = []
        with codecs.open(SCRAPY_PATH + '/normalized_scrapy_电视剧.txt', 'r') as fr:
            all_programs.append([line.strip() for line in fr.readlines()])
        with codecs.open(SCRAPY_PATH + '/normalized_documentary.txt', 'r') as fr:
            all_programs.append([line.strip() for line in fr.readlines()])
        with codecs.open(SCRAPY_PATH + '/normalized_scrapy_动漫.txt', 'r') as fr:
            all_programs.append([line.strip() for line in fr.readlines()])

        categories = ['zongyi', 'tiyu', 'news', 'cai', 'junshi', 'lvyou', 'shaoer', 'fazhi']
        for category in categories:
            with codecs.open(SCRAPY_PATH + '/dianshiyan_' + category + '.txt', 'r') as fr:
                all_programs.append([line.strip() for line in fr.readlines()])
        return all_programs

    def classify_by_gold_programs(self, prefix_programs):
        """
        classify program by programs crawled from tv websites
        :param prefix_programs:
        :return:
        """

        all_programs = self.read_gold_programs()
        correct_categories = ['电视剧', '纪实', '少儿', '综艺', '体育',
                          '新闻', '财经', '军事', '旅游', '少儿', '法制']

        classified_programs = []
        classified_prefixes = []
        handler = DistanceClassifyer()
        class getoutofloop(Exception): pass
        for prefix, programs in prefix_programs:
            flag = False
            min_programs, min_distances = [], []
            try:
                # find the best matched program
                for i in range(len(all_programs)):
                    program, distance = handler.find_min_distance(prefix, all_programs[i])
                    if distance == 1.0: # matched perfectly
                        category = correct_categories[i]
                        classified_prefixes.append(prefix)
                        for program in programs:
                            classified_programs.append(('1prefix', program, category))
                        flag = True
                        raise getoutofloop
                    min_programs.append(program)
                    min_distances.append(distance)
            except getoutofloop:
                pass

            if not flag:
                min_distance = max(min_distances)
                if min_distance > 0.93: # matched well
                    category = correct_categories[min_distances.index(min_distance)]
                    classified_prefixes.append(prefix)
                    for program in programs:
                        classified_programs.append(('1prefix', program, category))
        return classified_prefixes, classified_programs

    def crawl_to_search_prefixes(self, programs, N=6):
        """
        crawl relative programs info from xingchen
        :return:
        """

        if DEBUG: print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

        global empty_times
        empty_times = dict(zip(programs, [0 for _ in range(len(programs))]))

        source_programs = Manager().Queue()
        for program in programs:
            source_programs.put(program)

        processes = []
        pool, lock = Pool(processes=6), Manager().Lock()
        for _ in range(N):
            p = pool.apply_async(self.scrapyer.run_crawl_relative_programs, (source_programs, lock))
            processes.append(p)
        pool.close()
        pool.join()

        unabled_programs = []
        enabled_programs = []
        collected_programs = []
        for p in processes:
            res = p.get()
            collected_programs += res[0]
            enabled_programs += res[1]
            unabled_programs += res[2]

        # write search result into file
        with codecs.open(TMP_PATH + '/xingchen_unabled_programs.txt', 'w') as fw:
            fw.write('\n'.join(sorted(unabled_programs)))
        with codecs.open(TMP_PATH + '/xingchen_enabled_programs.txt', 'w') as fw:
            fw.write('\n'.join(sorted(enabled_programs)))
        with codecs.open(TMP_PATH + '/xingchen_collected_programs.txt', 'w') as fw:
            for item in collected_programs:
                fw.write(json.dumps(item, ensure_ascii=False) + '\n')

        if DEBUG: print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def find_best_matched(self, program, collected_programs):
        """
        find the best matched program from the collected programs
        :param program: source program
        :param collected_programs:
        :return:
        """

        # reorganize the list
        zongyi, tv, movie, sport, star = None, None, None, None, None
        for item in collected_programs.items():
            if item[0][:2] == '综艺':
                zongyi = item[1]
            elif item[0][:3] == '电视剧':
                tv = item[1]

        tmp_programs = []
        if zongyi: tmp_programs.append(('综艺', zongyi))
        if tv: tmp_programs.append(('电视剧', tv))

        # matched perfectly
        classify = Classifyer()
        for column, programs in tmp_programs:
            for href, name in programs:
                res = classify.preprocess_program(name)
                if res == program:
                    return 1, column, href, res

        # matched well
        handler = DistanceClassifyer()
        min_column, min_name, min_href, min_distance = '', '', '', 0
        for column, programs in tmp_programs:
            items = [classify.preprocess_program(pro) for _, pro in programs]
            name, distance = handler.find_min_distance(program, items)
            if distance > min_distance:
                min_name = name
                min_column = column
                min_distance = distance
                min_href = programs[items.index(name)][0]

        if min_distance > 0.93:
            # print(min_column, program, min_name)
            return 2, min_column, min_href, min_name
        return None

    def check_to_classify_programs(self):
        """
        classify part of the programs by check program column in xingchen
        :return: recrawl programs: need to crawl more detail information
                 unclassified: bad matched result, unable to classify
        """

        with codecs.open(TMP_PATH + '/xingchen_collected_programs.txt', 'r') as fr:
            enable_results = [json.loads(line.strip()) for line in fr]

        recrawl_programs = []
        unclassified_programs = []
        for result in enable_results:
            for program, collected_programs in result.items():
                res = self.find_best_matched(program, collected_programs)
                if not res:
                    unclassified_programs.append(program)
                    continue
                recrawl_programs.append((program, res[1], res[2]))
        return recrawl_programs, unclassified_programs

    def crawl_to_classify_programs(self, deep_crawl_programs, N=6):
        """
        crawl more detail info from xingchen to classify programs
        :param deep_crawl_programs: programs needs to crawl more info
        :param N: number of processes
        :return: classified result
        """

        pool = Pool()
        lock = Manager().Lock()
        source_items = Manager().Queue()
        for item in deep_crawl_programs:
            source_items.put(item)

        processes = []
        for _ in range(N):
            processes.append(pool.apply_async(self.scrapyer.run_search_to_classify_programs, (source_items, lock)))
        pool.close()
        pool.join()

        classified_results = []
        for p in processes:
            classified_results += p.get()
        return classified_results

    def classify_second(self, prefix_programs):
        """
        classify unclassify programs for the second step
        :param prefix_programs
        :return:
        """

        with open(TMP_PATH + '/prefix_classified_result.txt', 'r') as fr:
            items = [line.strip() for line in fr.readlines()]
            prefix_categories = [item.split('\t') for item in items]
            prefix_categories = dict(prefix_categories)

        classified_programs = []
        for prefix, programs in prefix_programs:
            category = prefix_categories.get(prefix, 'None')
            if category == 'None': continue
            for program in programs:
                classified_programs.append(('2prefix',  program, category))
        return classified_programs

    def merge_classify_prefix(self, prefix_classified):
        """
        merge the classified result with before result
        :param prefix_classified
        :return:
        """

        with open(RESULT_PATH + '/all_programs_category_1.txt', 'r') as fr:
            items = [line.strip() for line in fr.readlines()]
            classified_programs = [item.split('\t\t') for item in items]
            classified_programs = [(a, c, b) for a, b, c in classified_programs]
        with open(RESULT_PATH + '/reclassify_programs_1.txt', 'r') as fr:
            unclassify_programs = [line.strip() for line in fr.readlines()]

        unclassify_programs = set(unclassify_programs)
        classified_programs += prefix_classified
        unclassify_programs -= set([program for _, program, _ in prefix_classified])
        classified_programs = set(classified_programs)

        if DEBUG: print(len(classified_programs), len(unclassify_programs))
        if DEBUG: print(len(set([(program, category) for _, program, category in classified_programs])))
        if DEBUG: print(len(set([program for _, program, _ in classified_programs])))

        classified_programs = sorted(classified_programs, key=lambda item: (item[0], item[2], item[1]))
        with codecs.open(RESULT_PATH + '/reclassify_programs_2.txt', 'w') as fw:
            fw.write('\n'.join(sorted(unclassify_programs)))
        with codecs.open(RESULT_PATH + '/all_programs_category_2.txt', 'w') as fw:
            fw.write('\n'.join(['%s\t\t%s\t\t%s' % (a, c, b) for a, b, c in classified_programs]))


def run_prefix_classifyer():
    handler = PrefixClassifyer()

    prefix_programs = handler.search_common_prefix()
    matched_prefixes, classified_programs = handler.classify_by_gold_programs(prefix_programs)
    prefix_programs = [item for item in prefix_programs if not item[0] in matched_prefixes]

    prefixes_to_search = list(set(list(dict(prefix_programs).keys()))-set(matched_prefixes))
    handler.crawl_to_search_prefixes(prefixes_to_search)

    res_2 = handler.check_to_classify_programs()
    classified_result = handler.crawl_to_classify_programs(res_2[0])
    with codecs.open(TMP_PATH + '/prefix_classified_result.txt', 'w') as fw:
        fw.write('\n'.join(sorted(['\t'.join(item) for item in classified_result])))

    classified_programs += handler.classify_second(prefix_programs)
    handler.merge_classify_prefix(classified_programs)


if __name__ == '__main__':
    run_prefix_classifyer()
