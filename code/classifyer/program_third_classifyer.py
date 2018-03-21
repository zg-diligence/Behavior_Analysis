"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author:Gang Zhang
  Date:2018.02.01

  Function:
    1.classify all unclassified programs by matching classified programs

    2.correct the classified result by clustering and voting

    3.method for computing distance between programs can be improved
"""

import time
import heapq
import os, codecs
import Levenshtein
from multiprocessing import Pool

DEBUG = True
TMP_PATH = os.getcwd() + '/tmp_result'
SCRAPY_PATH = TMP_PATH + '/scrapy_programs'
RESULT_PATH = TMP_PATH + '/classified_result'
LAYER_PATH = TMP_PATH + '/similar_programs'


class DistanceClassifyer(object):
    """
    Function:
        1.classify the rest programs by matching classified programs
        2.correct the classified result by clustering and voting
        3.method for computing distance between programs can be improved
    """

    def __init__(self):
        pass

    def find_min_distance(self, program, gold_programs):
        """
        find the best matched program in gold_programs
        :param program: source program
        :param gold_programs: list
        :return: the best matched program and similarity
        """

        ldistances = [Levenshtein.jaro_winkler(program, item) for item in gold_programs]
        rdistances = [Levenshtein.jaro_winkler(program[::-1], item[::-1]) for item in gold_programs]
        lindex = ldistances.index(max(ldistances))
        rindex = rdistances.index(max(rdistances))
        ldistance = ldistances[lindex]
        rdistance = rdistances[rindex]
        if ldistance >= rdistance:
            return gold_programs[lindex], ldistance
        return gold_programs[rindex], rdistance

    def find_best_matched(self, program, classified_programs):
        """
        find the best matched program in gold_programs by voting
        the voting mechanism can be improved
        :param program: source program
        :param classified_programs: dict{program:category}
        :return:
        """

        gold_programs = classified_programs.keys()
        ldistances = [Levenshtein.jaro_winkler(program, item) for item in gold_programs]
        rdistances = [Levenshtein.jaro_winkler(program[::-1], item[::-1]) for item in gold_programs]

        items = list(zip(ldistances, gold_programs)) + list(zip(rdistances, gold_programs))
        maxn_items = heapq.nlargest(5, items, key=lambda item: item[0])
        maxn_items = [(item[0], item[1], classified_programs[item[1]]) for item in maxn_items]
        categories = list(set([item[2] for item in maxn_items]))
        scores = dict(zip(categories, [0 for _ in range(len(categories))]))

        for i in range(len(maxn_items)):
            _, _, category = maxn_items[i]
            scores[category] += len(maxn_items) - i
        scores = sorted(scores.items(), key=lambda item: item[1], reverse=True)

        category = scores[0][0]
        for item in maxn_items:
            if item[2] == category:
                similarity = item[0]
                min_program = item[1]
                return category, min_program, similarity

    def find_matched_programs(self, unclassify_programs, classified_programs):
        """
        find similar prorgrams between unclassify_programs and classified_programs
        :param unclassify_programs:
        :param classified_programs:
        :return:
        """

        counts = [0 for _ in range(9)]
        layer_programs = [[] for _ in range(9)] # save by matched degree

        for program in unclassify_programs:
            category, matched_program, distance = self.find_best_matched(program, classified_programs)
            if 0.95 <= distance:
                counts[0] += 1
                layer_programs[0].append(
                    (program, matched_program, str(distance), category))
            elif 0.90 <= distance < 0.95:
                counts[1] += 1
                layer_programs[1].append(
                    (program, matched_program, str(distance), category))
            elif 0.85 <= distance < 0.90:
                counts[2] += 1
                layer_programs[2].append(
                    (program, matched_program, str(distance), category))
            elif 0.80 <= distance < 0.85:
                counts[3] += 1
                layer_programs[3].append(
                    (program, matched_program, str(distance), category))
            elif 0.75 <= distance < 0.80:
                counts[4] += 1
                layer_programs[4].append(
                    (program, matched_program, str(distance), category))
            elif 0.70 <= distance < 0.75:
                counts[5] += 1
                layer_programs[5].append(
                    (program, matched_program, str(distance), category))
            elif 0.65 <= distance < 0.70:
                counts[6] += 1
                layer_programs[6].append(
                    (program, matched_program, str(distance), category))
            elif 0.60 <= distance < 0.65:
                counts[7] += 1
                layer_programs[7].append(
                    (program, matched_program, str(distance), category))
            else:
                counts[8] += 1
                layer_programs[8].append(
                    (program, matched_program, str(distance), category))

        return counts, layer_programs

    def classify_by_matched(self, unclassify_programs, classified_programs, N=4):
        """
        classify prorgams by matching classified and unclassify programs
        :param unclassify_programs
        :param classified_programs
        :param N: number of process
        :return:
        """

        if DEBUG: print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

        processes = []
        pool = Pool(4)
        basic_num = len(unclassify_programs) // N + 1
        for i in range(N):
            processes.append(pool.apply_async(self.find_matched_programs,
                (unclassify_programs[i * basic_num: i * basic_num + basic_num], classified_programs,)))
        pool.close()
        pool.join()

        counts = [0 for _ in range(9)]
        layer_programs = [[] for _ in range(9)]
        for p in processes:
            res = p.get()
            for i in range(9):
                counts[i] += res[0][i]
                layer_programs[i] += res[1][i]

        if DEBUG: print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

        if DEBUG: print(counts, sum(counts))
        for index, items in enumerate(layer_programs):
            with codecs.open(LAYER_PATH + '/dev_layer_programs_%d.txt' % index, 'w') as fw:
                fw.write('\n'.join(['\t'.join(item) for item in items]))
        return layer_programs

    def merge_classify_similarity(self, similarity_classified, result_num):
        """
        merge new classified programs with before result
        :param similarity_classified:
        :param result_num:
        :return:
        """

        with open(RESULT_PATH + '/all_programs_category_%d.txt'%result_num, 'r') as fr:
            items = [line.strip() for line in fr.readlines()]
            classified_programs = [item.split('\t\t') for item in items]
            classified_programs = [(a, c, b) for a, b, c in classified_programs]
        with open(RESULT_PATH + '/reclassify_programs_%d.txt'%result_num, 'r') as fr:
            unclassify_programs = [line.strip() for line in fr.readlines()]

        unclassify_programs = set(unclassify_programs)
        classified_programs += similarity_classified
        unclassify_programs -= set([program for _, program, _ in similarity_classified])
        classified_programs = set(classified_programs)

        if DEBUG: print(len(classified_programs), len(unclassify_programs))
        if DEBUG: print(len(set([(program, category) for _, program, category in classified_programs])))
        if DEBUG: print(len(set([program for _, program, _ in classified_programs])))

        classified_programs = sorted(classified_programs, key=lambda item: (item[0], item[2], item[1]))
        with codecs.open(RESULT_PATH + '/reclassify_programs_%d.txt'%(result_num+1), 'w') as fw:
            fw.write('\n'.join(sorted(unclassify_programs)))
        with codecs.open(RESULT_PATH + '/all_programs_category_%d.txt'%(result_num+1), 'w') as fw:
            fw.write('\n'.join(['%s\t\t%s\t\t%s' % (a, c, b) for a, b, c in classified_programs]))

    def correct_by_clustering(self):
        """
        correct classified result among programs with high similarity
        :return:
        """

        # with open(TMP_PATH + '/all_programs_category_3.txt', 'r') as fr:
        #     items = [line.strip() for line in fr.readlines()]
        #     classified_programs = [item.split('\t\t') for item in items]
        #     classified_programs = [(a, c, b) for a, b, c in classified_programs]
        # with open(TMP_PATH + '/reclassify_programs_3.txt', 'r') as fr:
        #     unclassify_programs = [line.strip() for line in fr.readlines()]

        # do clustering
        # voting with weight


if __name__ == '__main__':
    if not os.path.exists(LAYER_PATH):
        os.mkdir(LAYER_PATH)

    with open(RESULT_PATH + '/all_programs_category_5.txt', 'r') as fr:
        items = [line.strip() for line in fr.readlines()]
        classified_programs = [item.split('\t\t') for item in items]
        classified_programs = [(c, b) for a, b, c in classified_programs]

    with open(RESULT_PATH + '/reclassify_programs_5.txt', 'r') as fr:
        unclassify_programs = [line.strip() for line in fr.readlines()]

    tmp_category = ['新闻', '体育', '财经', '军事', '农业',
                    '纪实', '健康', '时尚', '美食','汽车',
                    '旅游', '综艺', '生活']

    classified_programs = [(a, b) for a, b in classified_programs if b in tmp_category]
    classified_programs = dict(classified_programs)

    handler = DistanceClassifyer()
    layer_programs = handler.classify_by_matched(unclassify_programs, classified_programs)

    classified_result = layer_programs[0] + layer_programs[1]
    classified_similarity = [('dev_1similarity', p, c) for p, _, _, c in classified_result]

    classified_result = layer_programs[2] + layer_programs[3] + layer_programs[4]
    classified_similarity += [('dev_2similarity', p, c) for p, _, _, c in classified_result]

    classified_result = layer_programs[5] + layer_programs[6] + layer_programs[7]
    classified_similarity += [('dev_3similarity', p, c) for p, _, _, c in classified_result]

    classified_result = layer_programs[8]
    classified_similarity += [('dev_4similarity', p, c) for p, _, _, c in classified_result]

    handler.merge_classify_similarity(classified_similarity, 5)
