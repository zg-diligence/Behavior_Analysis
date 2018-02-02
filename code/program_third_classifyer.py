import time
import os, codecs
import Levenshtein
from multiprocessing import Pool

DEBUG = True
TMP_PATH = os.getcwd() + '/tmp_result'
SCRAPY_PATH = TMP_PATH + '/scrapy_programs'

class DistanceClassifyer(object):
    def __init__(self):
        pass

    def find_min_distance(self, program, gold_programs):
        """
        find the best matched program in gold_programs
        :param program: source program
        :param gold_programs: classified programs
        :return:
        """

        distances_l = [Levenshtein.jaro_winkler(program, item) for item in gold_programs]
        distances_r = [Levenshtein.jaro_winkler(program[::-1], item[::-1]) for item in gold_programs]
        index_l = distances_l.index(max(distances_l))
        index_r = distances_r.index(max(distances_r))
        distance_l = distances_l[index_l]
        distance_r = distances_r[index_r]
        if distance_l >= distance_r:
            return gold_programs[index_l], distance_l
        return gold_programs[index_r], distance_r

    def find_similar_programs(self, unclassify_programs, classified_programs):
        counts = [0 for _ in range(8)]
        layer_programs = [[] for _ in range(8)]
        gold_programs = list(classified_programs.keys())

        for program in unclassify_programs:
            matched_program, distance = self.find_min_distance(program, gold_programs)
            if 0.95 <= distance:
                counts[0] += 1
                layer_programs[0].append(
                    (program, matched_program, str(distance), classified_programs[matched_program]))
            elif 0.90 <= distance < 0.95:
                counts[1] += 1
                layer_programs[1].append(
                    (program, matched_program, str(distance), classified_programs[matched_program]))
            elif 0.85 <= distance < 0.90:
                counts[2] += 1
                layer_programs[2].append(
                    (program, matched_program, str(distance), classified_programs[matched_program]))
            elif 0.80 <= distance < 0.85:
                counts[3] += 1
                layer_programs[3].append(
                    (program, matched_program, str(distance), classified_programs[matched_program]))
            elif 0.75 <= distance < 0.80:
                counts[4] += 1
                layer_programs[4].append(
                    (program, matched_program, str(distance), classified_programs[matched_program]))
            elif 0.70 <= distance < 0.75:
                counts[5] += 1
                layer_programs[5].append(
                    (program, matched_program, str(distance), classified_programs[matched_program]))
            elif 0.65 <= distance < 0.70:
                counts[6] += 1
                layer_programs[6].append(
                    (program, matched_program, str(distance), classified_programs[matched_program]))
            elif 0.60 <= distance < 0.65:
                counts[7] += 1
                layer_programs[7].append(
                    (program, matched_program, str(distance), classified_programs[matched_program]))

        return counts, layer_programs

    def match_similar_programs(self, N=4):
        if DEBUG: print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

        with open(TMP_PATH + '/all_programs_category_2.txt', 'r') as fr:
            items = [line.strip() for line in fr.readlines()]
            classified_programs = [item.split('\t\t') for item in items]
            classified_programs = dict([(c, b) for a, b, c in classified_programs])

        with open(TMP_PATH + '/reclassify_programs_2.txt', 'r') as fr:
            unclassify_programs = [line.strip() for line in fr.readlines()]

        processes = []
        pool = Pool(4)
        basic_num = len(unclassify_programs) // N + 1
        for i in range(N):
            processes.append(pool.apply_async(self.find_similar_programs,
                (unclassify_programs[i * basic_num: i * basic_num + basic_num], classified_programs,)))
        pool.close()
        pool.join()

        counts = [0 for _ in range(8)]
        layer_programs = [[] for _ in range(8)]
        for p in processes:
            res = p.get()
            for i in range(8):
                counts[i] += res[0][i]
                layer_programs[i] += res[1][i]

        if DEBUG: print(counts, sum(counts))
        for index, items in enumerate(layer_programs):
            with codecs.open(TMP_PATH + '/layer_programs_%d.txt' % index, 'w') as fw:
                fw.write('\n'.join(['\t'.join(item) for item in items]))

        if DEBUG: print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


if __name__ == '__main__':
    handler = DistanceClassifyer()
    handler.match_similar_programs()
