import os, re, json
import time, codecs
from redis import Redis
from queue import Queue
from bs4 import BeautifulSoup
from random import randint, choice
from redis.connection import BlockingConnectionPool
from urllib.request import urlopen, quote, Request, \
    ProxyHandler, build_opener, install_opener

from program_first_classifyer import Classifyer
from basic_category import categories as all_categories

TMP_PATH = os.getcwd() + '/tmp_result'

DEBUG = True
source_programs = Queue()
unabled_programs = list()
enabled_programs = list()
collected_programs = list()


class ProxyPool(object):
    def __init__(self):
        self.proxy = None
        self.session = Redis(connection_pool=BlockingConnectionPool(host='localhost', port=6379))
        self.headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/'
                                      '537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}

    def get_proxy(self):
        """
        get a new proxy from proxy pool
        :return:
        """

        while True:
            try:
                proxies = list(self.session.hgetall('useful_proxy').keys())
                new_proxy = choice(proxies).decode('utf8')
                break
            except:
                pass
        return new_proxy

    def delete_proxy(self, proxy):
        """
        delete the given proxy from proxy pool
        :param proxy:
        :return:
        """

        self.session.hdel('useful_proxy', proxy)

    def change_proxy(self):
        """
        change current proxy
        :return:
        """

        if self.proxy:
            self.delete_proxy(self.proxy)
        self.proxy = self.get_proxy()
        proxy_support = ProxyHandler({'http': self.proxy})
        opener = build_opener(proxy_support)
        opener.addheaders = [('User-Agent', self.headers['User-Agent'])]
        install_opener(opener)


class Scrapyer(object):
    def __init__(self, proxypool):
        self.retry_count = 3
        self.empty_count = 0
        self.pre_empty_flag = False
        self.proxypool = proxypool

    def check_empty(self, num):
        """
        check whether if the current proxy is dead
        if the res '[]' occurs over 5 times consecutively
        :param num: number of current columns
        :return:
        """

        if num == 0:
            if self.pre_empty_flag:
                self.empty_count += 1
                if self.empty_count >= 5:
                    for i in range(5, 0, -1):
                        program = unabled_programs[i]
                        if empty_times[program] < 2:
                            unabled_programs.pop(i)
                            source_programs.put(program)
                            empty_times[program] += 1
                    self.proxypool.change_proxy()
                    self.empty_count = 0
            else:
                self.pre_empty_flag = True
                self.empty_count = 1
        elif self.pre_empty_flag:
            self.pre_empty_flag = False
            self.empty_count = 0

    def collect_programs(self, page_uls, page_columns):
        """
        parse programs from the crawed result by category
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

    def crawl_relative_programs(self, program):
        """
        crawl relative programs info from http://www.tvmao.com
        :param program:
        :return:
        """

        url = 'http://www.tvmao.com/query.jsp?keys=%s&ed=' % quote(program) + \
              'bOWkp%2BeZveWkq%2BWmh%2BS4iua8lOazoeayq%2BS5i%2BWQu28%3D'

        bsObj = None
        self.retry_count = 3
        while self.retry_count > 0:
            try:
                html = urlopen(Request(url=url), timeout=2)
                bsObj = BeautifulSoup(html, 'html.parser')
                break
            except:
                self.retry_count -= 1
                if self.retry_count <= 0:
                    if DEBUG: print("Please waiting...")
                    self.proxypool.change_proxy()
                    self.retry_count = 3

        try:
            page_content = bsObj.find_all('div', class_='page-content')[0]
            page_columns = [item.a.get_text() for item in page_content.dl.find_all('dd')]
            page_columns = [column for column in page_columns if not re.search('^(播出时间|电视频道)', column)]
            page_content_uls = page_content.div.find_all('ul', class_=re.compile('^.+qtable$'), recursive=False)
            if DEBUG: print(page_columns)
            if len(page_columns) == 0:
                unabled_programs.append(program)
            else:
                enabled_programs.append(program)
                column_programs = self.collect_programs(page_content_uls, page_columns)
                if DEBUG: print(column_programs)
                collected_programs.append({program: column_programs})
            self.check_empty(len(page_columns))
        except Exception as e:
            if DEBUG: print('error2 =>', e)
            source_programs.put(program)
            self.proxypool.change_proxy()

    def category_classify(self, category):
        """
        classify by the info or category from xingchen
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
        classify life category into more accurate category
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
        classify programs by crawl more info from xingchen
        :param href: link of the relative program in xingchen
        :return:
        """

        bsObj = None
        self.retry_count = 3
        while self.retry_count > 0:
            try:
                html = urlopen(Request(url=href), timeout=2)
                bsObj = BeautifulSoup(html, 'html.parser')
                break
            except:
                self.retry_count -= 1
                if self.retry_count <= 0:
                    if DEBUG: print("Please waiting...")
                    self.proxypool.change_proxy()
                    self.retry_count = 3

        try:
            # variety category
            res_1 = bsObj.find_all('td', class_='gray pl15')
            if res_1:
                category = res_1[0].findNext('td').get_text()
                if category != "生活":
                    category = self.category_classify(category)
                    return category if category else '综艺'
                div = bsObj.find_all('div', class_='clear more_c')[0]
                intro = '; '.join([p.get_text() for p in div.find_all('p')])
                return self.intro_classify(intro)

            # TV category
            res_2 = bsObj.find_all('span', class_='gray font18')
            if res_2:
                td = bsObj.findAll(text='类别：')[0].parent.findNext('td')
                category = ' '.join([a.get_text() for a in td.find_all('a', recursive=False)])
                category = self.category_classify(category)
                return category if category else '电视剧'

            # others
            return '综艺'
        except Exception as e:
            if DEBUG: print('error2 =>', e, href)
            return None


class PrefixClassifier(object):
    def __init__(self, scrapyer):
        self.scrapyer = scrapyer

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
        find best prefix between continuously items
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
        search all possible prefixs among all programs
        :return:prefix_programs.txt prefix_lists.txt
        """

        with codecs.open(TMP_PATH + '/reclassify_programs.txt', 'r') as fr:
            programs = [line.strip() for line in fr.readlines()]

        prefixs, extracted_programs = [], []

        # search common prefix
        pre, cur = 0, 1
        N, max_length = 4, len(programs)
        while cur < max_length:
            pre_str, cur_str = programs[pre], programs[cur]
            prefix = self.get_common_prefix(pre_str, cur_str)
            if prefix:
                res_4 = self.get_best_prefix(programs, prefix, pre, cur, min_length=N)
                res_3 = self.get_best_prefix(programs, prefix, pre, cur, min_length=N-1)
                # res_2 = self.get_best_prefix(programs, prefix, pre, cur, min_length=N-2)
                res = res_4
                if len(res_3[3]) - len(res_4[3]) >= 5:
                    res = res_3
                    # if len(res_2[3]) - len(res_3[3]) >= threadhold:
                    #     res = res_2
                pre, cur = res[1], res[2]
                if res[0]:
                    prefixs.append(res[0])
                    extracted_programs.append(res[3])
            pre, cur = cur, cur + 1

        # normalize the prefixs
        handled_prefixs = []
        for prefix in prefixs:
            if re.search('的$', prefix):
                extracted_programs.pop(prefixs.index(prefix))
                continue

            res = prefix
            if re.search('\D\D(\d{1,2}|之)$', prefix):
                res = re.sub('(\d{1,2}|之)$', '', prefix)
            handled_prefixs.append(res)
        prefixs = handled_prefixs

        if DEBUG: print(sum([len(programs) for programs in extracted_programs]))
        with codecs.open(TMP_PATH + '/prefix_programs.txt', 'w') as fw:
            fw.write('\n'.join(sorted(set(prefixs))))
        with codecs.open(TMP_PATH + '/prefix_lists.txt', 'w') as fw:
            for prefix, programs in zip(prefixs, extracted_programs):
                fw.write('The prefix:' + prefix + '\n')
                fw.write('\n'.join(programs) + '\n\n')

    def crawl_to_search_programs(self):
        """
        crawl relative programs info from xingchen
        :return:
        """

        if DEBUG: print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

        with codecs.open(TMP_PATH + '/prefix_programs.txt', 'r') as fr:
            programs = [line.strip() for line in fr.readlines()]

        global empty_times
        for program in programs: source_programs.put(program)
        empty_times = dict(zip(programs, [0 for _ in range(len(programs))]))

        try:
            while True:
                program = source_programs.get_nowait()
                if DEBUG: print(); print(source_programs.qsize(), '=>', program)
                self.scrapyer.crawl_relative_programs(program)
                time.sleep(randint(0, 1))
        except:
            pass

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
        find best matched program from the collected programs
        1, 2 call classify_xinchen_category
        3, already can be classified;
        4, craw detail info to classify
        :param program: source program
        :param collected_programs:
        :return:
        """

        zongyi, tv, movie, sport, star = None, None, None, None, None
        for item in collected_programs.items():
            if item[0][:2] == '综艺':
                zongyi = item[1]
            elif item[0][:3] == '电视剧':
                tv = item[1]
            elif item[0][:2] == '电影':
                movie = item[1]
            elif item[0][:2] == '赛事':
                sport = item[1]
            elif item[0][:2] == '明星':
                star = item[1]
        tmp_programs = []
        if zongyi: tmp_programs.append(('综艺', zongyi))
        if tv: tmp_programs.append(('电视剧', tv))
        if movie: tmp_programs.append(('电影', movie))
        if sport: tmp_programs.append(('体育', sport))
        if star: tmp_programs.append(('综艺', star))

        classify = Classifyer()
        for column, programs in tmp_programs:
            for href, name in programs:
                res = classify.preprocess_program(name)
                if res == program:
                    return 1, column, href, res

        for column, programs in tmp_programs:
            for href, name in programs:
                if re.search('^%s|%s$' % (program, program), name):
                    return 2, column, href, name

        if len(tmp_programs) == 1:
            if tmp_programs[0][0] == '赛事': return 3, '体育', None, None
            if tmp_programs[0][0] == '明星': return 3, '综艺', None, None
            if tmp_programs[0][0] == '电影': return 3, '电影', None, None
            if re.search('^(电视剧|综艺)', tmp_programs[0][0]):
                href = tmp_programs[0][1][0][0]  # 待定
                name = tmp_programs[0][1][0][1]  # 待定
                return 4, tmp_programs[0][0], href, name

        return None

    def classify_xinchen_category(self, column):
        """
        classify program roughly by the xingchen column
        :param column:
        :return:
        """

        if re.search('^电影', column): return '电影'
        if re.search('^赛事', column): return '体育'
        if re.search('^明星', column): return '综艺'
        return "继续"

    def check_to_classify_programs(self):
        """
        classify part of the programs by check program column in xingchen
        :return: recrawl programs: need to cral more detail information
                 classified programs: already can be classified
                 unclassified: bad matched result, unable to classify
        """

        with codecs.open(TMP_PATH + '/xingchen_collected_programs.txt', 'r') as fr:
            enable_results = [json.loads(line.strip()) for line in fr]

        recrawl_programs = []
        classified_programs = []
        unclassified_programs = []
        for result in enable_results:
            for program, collected_programs in result.items():
                res = self.find_best_matched(program, collected_programs)
                if not res:
                    unclassified_programs.append(program)
                    continue

                if res[0] == 3:
                    classified_programs.append((program, res[1]))
                    continue

                if res[0] == 4:
                    recrawl_programs.append((program, res[1], res[2]))
                    continue

                category = self.classify_xinchen_category(res[1])
                if category == '继续':
                    recrawl_programs.append((program, res[1], res[2]))
                else:
                    classified_programs.append((program, category))
        return recrawl_programs, classified_programs, unclassified_programs

    def crawl_to_classify_programs(self, deep_crawl_programs):
        """
        crawl more detail info from xingchen to classify programs
        :param deep_crawl_programs: programs needs to craw more info
        :return: classified result
        """

        try:
            classified_results = []
            for item in deep_crawl_programs:
                category = self.scrapyer.search_to_classify_program(item[2])
                if DEBUG: print((item[0], category))
                classified_results.append((item[0], category))
        except:
            pass
        else:
            return classified_results

    def classify_second(self):
        """
        read prefixs crawled result and classify the relative programs
        :return:
        """

        with open(TMP_PATH + '/prefix_lists.txt', 'r') as fr:
            items = [line.strip() for line in fr.readlines()]

            groups, group = [], []
            for item in items:
                if item:
                    group.append(item)
                else:
                    groups.append(group)
                    group = []

            keymaps = []
            for group in groups:
                prefix = group[0].split(':')[1]
                programs = group[1:]
                keymaps.append((prefix, programs))
            keymaps = dict(keymaps)

        with open(TMP_PATH + '/prefix_classified_result.txt', 'r') as fr:
            items = [line.strip() for line in fr.readlines()]
            prefix_categories = [item.split('\t') for item in items]
            prefix_categories = dict(prefix_categories)

        classified_programs = []
        for prefix, programs in keymaps.items():
            category = prefix_categories.get(prefix, 'None')
            if category == 'None': continue
            for program in programs:
                classified_programs.append(('3prefix',  program, category))
        return classified_programs

    def merge_classify_prefix(self):
        """
        merge the classified result from class_programs.py
        :return:
        """

        with open(TMP_PATH + '/all_programs_category.txt', 'r') as fr:
            items = [line.strip() for line in fr.readlines()]
            classified_programs = [item.split('\t\t') for item in items]
            classified_programs = [(a, c, b) for a, b, c in classified_programs]
        with open(TMP_PATH + '/reclassify_programs.txt', 'r') as fr:
            unclassify_programs = [line.strip() for line in fr.readlines()]

        unclassify_programs = set(unclassify_programs)
        prefix_classified = self.classify_second()
        classified_programs += prefix_classified
        unclassify_programs -= set([program for _, program, _ in prefix_classified])
        classified_programs = set(classified_programs)

        if DEBUG: print(len(classified_programs), len(unclassify_programs))
        if DEBUG: print(len(set([(program, category) for _, program, category in classified_programs])))
        if DEBUG: print(len(set([program for _, program, _ in classified_programs])))

        classified_programs = sorted(classified_programs, key=lambda item: (item[0], item[2], item[1]))
        with codecs.open(TMP_PATH + '/reclassify_programs_2.txt', 'w') as fw:
            fw.write('\n'.join(sorted(unclassify_programs)))
        with codecs.open(TMP_PATH + '/all_programs_category_2.txt', 'w') as fw:
            fw.write('\n'.join(['%s\t\t%s\t\t%s' % (a, c, b) for a, b, c in classified_programs]))


if __name__ == '__main__':
    proxypool = ProxyPool()
    proxypool.change_proxy()
    scrapyer = Scrapyer(proxypool)
    handler = PrefixClassifier(scrapyer)

    # res_1 = handler.crawl_to_search_programs()
    # res_2 = handler.check_to_classify_programs()
    # res_3 = handler.crawl_to_classify_programs(res_2[0])
    # classified_result = res_2[1] + res_3
    # with codecs.open(TMP_PATH + '/prefix_classified_result.txt', 'w') as fw:
    #     fw.write('\n'.join(sorted(['\t'.join(item) for item in classified_result])))

    handler.search_common_prefix()
    handler.classify_second()
    handler.merge_classify_prefix()
