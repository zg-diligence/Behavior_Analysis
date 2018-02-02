import time,json
import os, codecs
import Levenshtein
from multiprocessing import Pool

DEBUG = True
TMP_PATH = os.getcwd() + '/tmp_result'
SCRAPY_PATH = TMP_PATH + '/scrapy_programs'

class DistanceClassifyer(object):
    def __init__(self):
        pass

    def compute_similarity(self, src_programs, des_programs):
        """
        compute similarity between src_programs, des_programs
        :param src_programs:
        :param des_programs:
        :return:
        """

        with open('%s_tmp.txt' % os.getpid(), 'a') as fw:
            for program in src_programs:
                ldistances = [float('%.3f'%Levenshtein.jaro_winkler(program, item)) for item in des_programs]
                rdistances = [float('%.3f'%Levenshtein.jaro_winkler(program[::-1], item[::-1])) for item in des_programs]
                item = {program:(ldistances, rdistances)}
                fw.write(json.dumps(item, ensure_ascii=False) + '\n')
        return '%s_tmp.txt' % os.getpid()

    def run_compute_similarity(self, src_programs, des_programs, process_num=4):
        """
        compute similarity between src_programs, des_programs
        :param src_programs:
        :param des_programs:
        :param process_num
        :return:
        """

        processes = []
        pool = Pool(4)
        basic_num = len(src_programs) // process_num + 1
        for i in range(process_num):
            processes.append(pool.apply_async(self.compute_similarity,
                (src_programs[i * basic_num: i * basic_num + basic_num], des_programs,)))
        pool.close()
        pool.join()

        # similarity_matrix = []
        # for p in processes:
        #     similarity_matrix += p.get()
        # return similarity_matrix

    def find_best_matched(self, similaritys, des_programs):
        """
        find the best matched program in des_programs
        :param similaritys: similaritys from current program to all des_programs
        :param des_programs
        :return:
        """

        similaritys_l, similaritys_r = similaritys
        ldistances = [float('%.3f' % Levenshtein.jaro_winkler(program, item)) for item in des_programs]
        rdistances = [float('%.3f' % Levenshtein.jaro_winkler(program[::-1], item[::-1])) for item in des_programs]
        index_l = similaritys_l.index(max(similaritys_l))
        index_r = similaritys_r.index(max(similaritys_r))
        similarity_l = similaritys_l[index_l]
        similarity_r = similaritys_r[index_r]
        if similarity_l >= similarity_r:
            return des_programs[index_l], similarity_l
        return des_programs[index_r], similarity_r

    def find_similar_programs(self, unclassify_programs, classified_programs):
        """
        match programs between classified and unclassify programs
        :param unclassify_programs:
        :param classified_programs:
        :return:
        """

        counts = [0 for _ in range(8)]
        layer_programs = [[] for _ in range(8)]
        gold_programs = list(classified_programs.keys())
        similarity_matrix = self.compute_similarity(unclassify_programs, gold_programs)
        for i in range(len(unclassify_programs)):
            program = unclassify_programs[i]
            matched_program, similarity = self.find_best_matched(similarity_matrix[i], gold_programs)
            if 0.95 <= similarity:
                counts[0] += 1
                layer_programs[0].append(
                    (program, matched_program, str(similarity), classified_programs[matched_program]))
            elif 0.90 <= similarity < 0.95:
                counts[1] += 1
                layer_programs[1].append(
                    (program, matched_program, str(similarity), classified_programs[matched_program]))
            elif 0.85 <= similarity < 0.90:
                counts[2] += 1
                layer_programs[2].append(
                    (program, matched_program, str(similarity), classified_programs[matched_program]))
            elif 0.80 <= similarity < 0.85:
                counts[3] += 1
                layer_programs[3].append(
                    (program, matched_program, str(similarity), classified_programs[matched_program]))
            elif 0.75 <= similarity < 0.80:
                counts[4] += 1
                layer_programs[4].append(
                    (program, matched_program, str(similarity), classified_programs[matched_program]))
            elif 0.70 <= similarity < 0.75:
                counts[5] += 1
                layer_programs[5].append(
                    (program, matched_program, str(similarity), classified_programs[matched_program]))
            elif 0.65 <= similarity < 0.70:
                counts[6] += 1
                layer_programs[6].append(
                    (program, matched_program, str(similarity), classified_programs[matched_program]))
            elif 0.60 <= similarity < 0.65:
                counts[7] += 1
                layer_programs[7].append(
                    (program, matched_program, str(similarity), classified_programs[matched_program]))

        return counts, layer_programs

    def classify_by_matched(self, N=4):
        """
        classify prorgams by matching classified and unclassify programs
        :param N: number of process
        :return:
        """

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
        if DEBUG: print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

        if DEBUG: print(counts, sum(counts))
        for index, items in enumerate(layer_programs):
            with codecs.open(TMP_PATH + '/layer_programs_%d.txt' % index, 'w') as fw:
                fw.write('\n'.join(['\t'.join(item) for item in items]))
        return layer_programs

    def merge_classify_similarity(self, similarity_classified):
        """
        merge new classified programs with before result
        :param similarity_classified:
        :return:
        """

        with open(TMP_PATH + '/all_programs_category_2.txt', 'r') as fr:
            items = [line.strip() for line in fr.readlines()]
            classified_programs = [item.split('\t\t') for item in items]
            classified_programs = [(a, c, b) for a, b, c in classified_programs]
        with open(TMP_PATH + '/reclassify_programs_2.txt', 'r') as fr:
            unclassify_programs = [line.strip() for line in fr.readlines()]

        unclassify_programs = set(unclassify_programs)
        classified_programs += similarity_classified
        unclassify_programs -= set([program for _, program, _ in similarity_classified])
        classified_programs = set(classified_programs)

        if DEBUG: print(len(classified_programs), len(unclassify_programs))
        if DEBUG: print(len(set([(program, category) for _, program, category in classified_programs])))
        if DEBUG: print(len(set([program for _, program, _ in classified_programs])))

        classified_programs = sorted(classified_programs, key=lambda item: (item[0], item[2], item[1]))
        with codecs.open(TMP_PATH + '/reclassify_programs_3.txt', 'w') as fw:
            fw.write('\n'.join(sorted(unclassify_programs)))
        with codecs.open(TMP_PATH + '/all_programs_category_3.txt', 'w') as fw:
            fw.write('\n'.join(['%s\t\t%s\t\t%s' % (a, c, b) for a, b, c in classified_programs]))

    def correct_by_clustering(self):
        """
        correct classified result among programs with high similarity
        :return:
        """

        with open(TMP_PATH + '/all_programs_category_2.txt', 'r') as fr:
            items = [line.strip() for line in fr.readlines()]
            classified_programs = [item.split('\t\t') for item in items]
            classified_programs = [(a, c, b) for a, b, c in classified_programs]
        with open(TMP_PATH + '/reclassify_programs_2.txt', 'r') as fr:
            unclassify_programs = [line.strip() for line in fr.readlines()]

        pass


if __name__ == '__main__':
    with open(TMP_PATH + '/all_programs_category_2.txt', 'r') as fr:
        items = [line.strip() for line in fr.readlines()]
        classified_programs = [item.split('\t\t') for item in items]
        classified_programs = dict([(c, b) for a, b, c in classified_programs])

    with open(TMP_PATH + '/reclassify_programs_2.txt', 'r') as fr:
        unclassify_programs = [line.strip() for line in fr.readlines()]

    handler = DistanceClassifyer()
    handler.run_compute_similarity(unclassify_programs, list(classified_programs.keys()))

    # layer_programs = handler.classify_by_matched()
    # classified_result = layer_programs[0] + layer_programs[1]
    # classified_similarity = [('1similarity', p, c) for p, _, _, c in classified_result]
    # handler.merge_classify_similarity(classified_similarity)
