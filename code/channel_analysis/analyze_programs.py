"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author: Gang Zhang
  Creation Date: 2018.4.12
  Last Modified: 2018.4.13

  Function:
    1.statistic watching duration of programs per day
    2.generate programs cloud by watching duration
    3.show the category proportion of programs per day
"""

import os
import re
import multiprocessing
from random import shuffle
from datetime import datetime
from wordcloud import WordCloud
from matplotlib import pyplot as plt
from matplotlib import colors as mcolors
from string import punctuation as env_punc
from zhon.hanzi import punctuation as chs_punc

plt.rcParams['font.sans-serif'] = ['FZKai-Z03']

DEBUG = True
EXTRACT_PATH = '/media/gzhang/Data/channel_data'
TMP_PATH = os.path.join(os.getcwd(), 'tmp_result')


def regex_for_normalize_programs():
    """
    regexies for normalizing programs
    :return: list
    """

    regexes = []
    chs_num = '一二三四五六七八九十'
    punctuations = env_punc + chs_punc
    unvisible_chars = ''.join([chr(i) for i in range(32)])

    #  # remove program marks
    regexes.append(re.compile('.*(报复|反复|回复|修复)$'))
    regexes.append(re.compile('复$'))
    regexes.append(re.compile('现场直播|实况录像|免费'))
    regexes.append(re.compile('(中文版|英文版|回看|复播|重播|[上中下尾]|[ⅡⅢI]+)$'))
    regexes.append(re.compile('^(HD|3D|杜比)|(SD文广版|文广(遮标)*版|SD|HD|3D|TV|杜比|限免|done|out)$'))

    # remove date
    r1 = '(19|20)*[0-9][0-9](年|-)(0[1-9]|10|11|12|[0-9])(月|-)(0[1-9]|[1-2][0-9]|3[0-1]|[0-9])日*'
    r2 = '(19|20)*[0-9][0-9](年|-)*(0[1-9]|10|11|12)(月|-)*(0[0-9]|[1-2][0-9]|3[0-1])日*\d*'
    r3 = '(0[1-9]|10|11|12|[1-9])月(0[1-9]|[1-2][0-9]|3[0-1]|[1-9])日'
    r4 = '(0[1-9]|10|11|12)(0[1-9]|[1-2][0-9]|3[0-1])'
    r7 = '(19|20)[0-9][0-9]-*\d*'
    r5 = '(19|20)[0-9][0-9]年'
    r6 = '(19|20)[0-9][0-9](-|/)(19|20)[0-9][0-9]'
    regexes.append(re.compile('(%s|%s|%s|%s|%s|%s|%s)' % (r1, r2, r3, r4, r5, r6, r7)))

    # remove space/punctuations/control chars
    regexes.append(re.compile('\s'))
    regexes.append(re.compile('[%s]' % punctuations))
    regexes.append(re.compile('[%s]' % unvisible_chars))

    # remove serial number
    regexes.append(re.compile('第*([%s]+|\d+)[期部季集]+' % chs_num))
    regexes.append(re.compile('(\d+|[%s]+)$' % chs_num))

    return regexes


def preprocess_program(program):
    """
    preprocess of program
    :param program:
    :return:
    """

    chs_num = '一二三四五六七八九十'
    regexes = regex_for_normalize_programs()

    # remove serial number in the middle of the program name
    if re.match('^\D+第([%s]+|\d+)[部集季]+.*$' % chs_num, program):
        res = re.search('第([%s]+|\d+)[部集季]+' % chs_num, program)
        program = program[:res.span()[0]]

    for regex in regexes[2:]:
        program = re.sub(regex, '', program)
    if not re.match(regexes[0], program):
        program = re.sub(regexes[1], '', program)

    for regex in regexes[2:]:
        program = re.sub(regex, '', program)
    if not re.match(regexes[0], program):
        program = re.sub(regexes[1], '', program)

    # remove chinese garbled
    if re.search('[^(\w+\-)]', program):
        return None

    return None if not program else program


class ProgramRatings(object):
    def __init__(self):
        pass

    def preprocess_prog(self, program):
        """
        normalize program
        :param program: original name of program
        :return: normalized program
        """

        if re.match('^\d+-.*-.*$', program):
            return '音乐播放'
        new_prog = preprocess_program(program)
        return new_prog

    def compute_seconds(self, channel_path, threshold):
        """
        counting time for programs in one channel
        :param channel_path: folder path of the day for one channel
        :param threshold: time limit of a valid watching record
        :return: dict for (program, time)
        """

        if DEBUG: print(channel_path)
        hour_files = sorted(os.listdir(channel_path))
        prog_times = {}
        for filename in hour_files:
            file_path = os.path.join(channel_path, filename)
            with open(file_path) as fr:
                for line in fr:
                    try:
                        items = line.strip().split('|')
                        start_tt = datetime.strptime(items[6], '%Y.%m.%d %H:%M:%S')
                        end_tt = datetime.strptime(items[5], '%Y.%m.%d %H:%M:%S')
                        init = datetime(year=2016, month=5, day=1, hour=0, minute=0, second=0)
                        start_tt = start_tt if start_tt >= init else init
                        seconds = (end_tt - start_tt).total_seconds()
                        if seconds < threshold: continue
                        program = self.preprocess_prog(items[11])
                        if not prog_times.get(program, None):
                            prog_times[program] = 0.0
                        prog_times[program] += seconds
                    except:
                        continue
        return prog_times

    def compute_for_day(self, catelogue, day, threshold=15):
        """
        count watching time for one day
        :param catelogue: root catelogue of source data
        :param day: the pointed day
        :param threshold: time limit of a valid watching record
        :return:
        """

        channel_folders = os.listdir(catelogue)
        channel_folders = [os.path.join(catelogue, folder, day) for folder in channel_folders]

        processes = []
        pool = multiprocessing.Pool(4)
        for folder in channel_folders:
            processes.append(pool.apply_async(self.compute_seconds, (folder, threshold)))
        pool.close()
        pool.join()

        prog_times = {}
        for p in processes:
            res = p.get()
            for prog, times in res.items():
                if not prog_times.get(prog, None):
                    prog_times[prog] = 0.0
                prog_times[prog] += times
        prog_times = sorted(prog_times.items(), key=lambda item: item[1], reverse=True)

        with open(TMP_PATH + '/prog_times_' + day + '.txt', 'w') as fw:
            for prog, time in prog_times:
                fw.write('%-30s%-10.0f\n' % (prog, time))

    def generate_prog_cloud(self, prog_times_path):
        """
        generate programs cloud
        :param prog_times_path: file path of statistic result of program
        :return: none
        """

        with open(prog_times_path) as fr:
            prog_times = [re.split('\s+', line.strip()) for line in fr]
            prog_times = [(a, int(b.split('.')[0]) // 10000) for a, b in prog_times]
        generated_list = []
        for prog, times in prog_times:
            generated_list += [prog] * times
        shuffle(generated_list)

        plt.figure()
        wl_space_split = " ".join(generated_list)
        cloud = WordCloud(
            width=800,
            height=600,
            max_words=20000,
            min_font_size=20,
            max_font_size=120,
            font_path="simhei.ttf",
            background_color='black'
        )
        my_wordcloud = cloud.generate(wl_space_split)
        plt.imshow(my_wordcloud)
        plt.axis("off")
        day = prog_times_path[-6:-4]
        plt.savefig(TMP_PATH + '/prog_cloud_' + day + '.jpg', format='jpg', dpi=600)

    def show_classification_ratings(self, prog_times_path, prog_dict):
        """
        show the category proportion of programs
        :param prog_times_path: file path of statistic result of program
        :param prog_dict: dict of program and its category
        :return: none
        """

        with open(prog_times_path) as fr:
            prog_times = [re.split('\s+', line.strip()) for line in fr]
            prog_times = [(a, int(b.split('.')[0])) for a, b in prog_times]

        cate_times = {}
        for prog, time in prog_times:
            if prog == 'None': continue
            try:
                cate = prog_dict[prog]
            except:
                continue
            else:
                if not cate_times.get(cate, None):
                    cate_times[cate] = 0
                cate_times[cate] += time
        cate_times = sorted(cate_times.items(), key=lambda item: item[1], reverse=True)
        sum_times = sum([time for _, time in cate_times])
        cate_proportion = [(cate, time / sum_times) for cate, time in cate_times]
        prefer_proportion = [prop for _, prop in cate_proportion[:8]]
        prefer_proportion.append(1 - sum(prefer_proportion))

        labels = [cat for cat, _ in cate_proportion[:8]] + ['其它', ]
        colors = dict(mcolors.BASE_COLORS, **mcolors.CSS4_COLORS)
        colors = sorted([item for item in colors.values() if type(item) != tuple])
        colors = [colors[1 + i * 13] for i in range(len(labels))]

        plt.figure(figsize=(8, 6))
        plt.pie(prefer_proportion, labels=labels, colors=colors,
                autopct='%4.1f%%', shadow=False, startangle=90)
        plt.axis('equal')
        day = prog_times_path[-6:-4]
        plt.savefig(TMP_PATH + '/prefer_category_' + day + '.jpg', format='jpg', dpi=600)


if __name__ == '__main__':
    handler = ProgramRatings()
    handler.compute_for_day(EXTRACT_PATH, '02')
    handler.generate_prog_cloud(TMP_PATH + '/prog_times_02.txt')

    with open(TMP_PATH + '/nprograms_dict.txt') as fr:
        cate_prog = [line.strip().split('\t\t') for line in fr]
        prog_cate = [(item[1], item[0]) for item in cate_prog]
        prog_dict = dict(prog_cate)
    prog_dict['音乐'] = '音乐'
    handler.show_classification_ratings(TMP_PATH + '/prog_times_02.txt', prog_dict)
