"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author:Gang Zhang
  Date:2018.02.01

  Function:
    1.classify short programs by crawling info from http://www.tvmao.com

    2.do some post-process to improve accuracy
"""

import os, re, json
import time, codecs
from multiprocessing import Pool, Manager

from program_second_classifyer import Scrapyer, Classifyer
from basic_category import categories as country_keywords
from program_third_classifyer import DistanceClassifyer

DEBUG = True
TMP_PATH = os.getcwd() + '/tmp_result'
SCRAPY_PATH = TMP_PATH + '/scrapy_programs'
RESULT_PATH = TMP_PATH + '/classified_result'


class ShortProgramClassifer(object):
    """
    Function:
        1.classify short programs by crawling info from http://www.tvmao.com
        2.do some post-process to improve accuracy
    """

    def __init__(self):
        self.scrapyer = Scrapyer()

    def post_processing(self):
        """
        do som post process to improve accuracy
        :return:
        """

        with open(RESULT_PATH + '/reclassify_programs_3.txt', 'r') as fr:
            unclassified_programs = [line.strip() for line in fr.readlines()]

        classified_programs = []
        for program in unclassified_programs:
            if re.search('^%s'%country_keywords, program):
                if program[:3] != '中国梦':
                    classified_programs.append(('post', program, '新闻'))
        return classified_programs

    def crawl_short_programs(self, N=4):
        """
        crawl relative programs info of short unclassified programs
        :param N: number of processes
        :return:
        """

        with open(RESULT_PATH + '/reclassify_programs_3.txt', 'r') as fr:
            unclassified_programs = [line.strip() for line in fr.readlines()]

        # extrct short programs, size of the program between 3 and 6
        short_programs = []
        for program in unclassified_programs:
            if re.search('^([0-9]|[A-Z]|[a-z])', program):
                continue
            if 2 < len(program) <=6 :
                short_programs.append(program)
        short_programs = list(set(short_programs))

        global empty_times
        empty_times = dict(zip(short_programs, [0 for _ in range(len(short_programs))]))

        # crawl relative programs info
        source_programs = Manager().Queue()
        for program in short_programs:
            source_programs.put(program)

        processes = []
        pool, lock = Pool(processes=6), Manager().Lock()
        for _ in range(N):
            p = pool.apply_async(self.scrapyer.run_crawl_relative_programs, (source_programs, lock, True))
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

        # write crawled result info file
        with codecs.open(TMP_PATH + '/xingchen_unabled_short_programs.txt', 'w') as fw:
            fw.write('\n'.join(sorted(unabled_programs)))
        with codecs.open(TMP_PATH + '/xingchen_enabled_short_programs.txt', 'w') as fw:
            fw.write('\n'.join(sorted(enabled_programs)))
        with codecs.open(TMP_PATH + '/xingchen_collected_short_programs.txt', 'w') as fw:
            for item in collected_programs:
                fw.write(json.dumps(item, ensure_ascii=False) + '\n')

        if DEBUG: print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def find_best_matched(self, program, collected_programs):
        """
        find best matched program from the collected programs
        1, 2 call classify_xingchen_category
        3, already can be classified
        4, crawl detail info to classify
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

        # others
        if len(tmp_programs) == 1:
            if tmp_programs[0][0] == '赛事': return 3, '体育', None, None
            if tmp_programs[0][0] == '明星': return 3, '综艺', None, None
            if tmp_programs[0][0] == '电影': return 3, '电影', None, None
            if re.search('^(电视剧|综艺)', tmp_programs[0][0]):
                href = tmp_programs[0][1][0][0]  # 待定
                name = tmp_programs[0][1][0][1]  # 待定
                return 4, tmp_programs[0][0], href, name
        return None

    def classify_xingchen_category(self, column):
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

                category = self.classify_xingchen_category(res[1])
                if category == '继续':
                    recrawl_programs.append((program, res[1], res[2]))
                else:
                    classified_programs.append((program, category))
        return recrawl_programs, classified_programs, unclassified_programs

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
        classified_programs = [('short', pro, cat) for pro, cat in classified_results]
        return classified_programs

    def merge_classify_post(self, new_classified):
        """
        merge the classified result with before
        :param new_classified
        :return:
        """

        with open(RESULT_PATH + '/all_programs_category_3.txt', 'r') as fr:
            items = [line.strip() for line in fr.readlines()]
            classified_programs = [item.split('\t\t') for item in items]
            classified_programs = [(a, c, b) for a, b, c in classified_programs]
        with open(RESULT_PATH + '/reclassify_programs_3.txt', 'r') as fr:
            unclassify_programs = [line.strip() for line in fr.readlines()]

        unclassify_programs = set(unclassify_programs)
        classified_programs += new_classified
        unclassify_programs -= set([program for _, program, _ in new_classified])
        classified_programs = set(classified_programs)

        if DEBUG: print(len(classified_programs), len(unclassify_programs))
        if DEBUG: print(len(set([(program, category) for _, program, category in classified_programs])))
        if DEBUG: print(len(set([program for _, program, _ in classified_programs])))

        classified_programs = sorted(classified_programs, key=lambda item: (item[0], item[2], item[1]))
        with codecs.open(RESULT_PATH + '/reclassify_programs_4.txt', 'w') as fw:
            fw.write('\n'.join(sorted(unclassify_programs)))
        with codecs.open(RESULT_PATH + '/all_programs_category_4.txt', 'w') as fw:
            fw.write('\n'.join(['%s\t\t%s\t\t%s' % (a, c, b) for a, b, c in classified_programs]))


if __name__ == '__main__':
    handler = ShortProgramClassifer()
    new_classified = handler.post_processing()

    handler.crawl_short_programs()
    res_1 = handler.check_to_classify_programs()
    new_classified += res_1[1]
    new_classified += handler.crawl_to_classify_programs(res_1[0])
    handler.merge_classify_post(new_classified)
