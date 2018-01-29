import os
import re
import codecs
from string import punctuation as env_punc
from zhon.hanzi import punctuation as chs_punc

from extract_channel_programs import Preprocess
from basic_category import classified_channels, sports_keywords

DEBUG = True
threshold = 4
TMP_PATH = os.getcwd() + '/tmp_result'
SCRAPY_PATH = TMP_PATH + '/scrapy_programs'
EXTRACT_CHANNEL_PROGRAM = TMP_PATH + '/extract_channel_program'


class Classify(object):
    def __init__(self):
        self.handler = Preprocess()
        with codecs.open('BA_all_channels.txt', 'r', encoding='utf8') as fr:
            self.all_channels = [line.strip() for line in fr.readlines()]

    def classify_exist_channels(self, file_path):
        """
        classify all exist channels roughly
        :param file_path:path the target file
        """

        with codecs.open(file_path, 'r', encoding='utf8') as fr:
            regexs = ['^(CCTV|中央)', '卫视$', '影院|电影',
                      '广播|电台|调频|频率|之声$|FM$|AM$',
                      '剧场', '音乐', '纪实|纪录', '音乐', '购',
                      '试播|通道|试验|体验|测试|TEST|test', '\w+']

            channels = [line.strip() for line in fr]
            categories = [[] for _ in range(len(regexs) + 1)]
            for channel in channels:
                for i in range(len(regexs)):
                    if re.search(regexs[i], channel, re.I):
                        categories[i].append(channel)
                        break

            with codecs.open("tmp_result/nmanual_classified_channels.txt", 'w', encoding='utf8') as fw:
                for i in range(len(categories)):
                    fw.write('\n'.join(sorted(set(categories[i]))) + '\n\n')

    def preprocess_channel(self, channel):
        # remove punctuations
        punctuations = env_punc + chs_punc
        channel = re.sub('[%s]' % punctuations, '', channel)

        # channel includes chinese garbled or is purely made up with number
        if re.search('[^(\w+\-)]', channel) or re.match('^[0-9]+$', channel):
            return None

        # remove Dolby、HD、高清 channels
        if len(channel) >= 5 and re.search('(高清|频道|HD)$', channel):
            channel = channel[:-2]
        elif re.search('Dolby$', channel):
            channel = channel[:-5]
        elif re.search('^NVOD', channel) and channel != 'NVOD4K':
            channel = channel[4:]
        return channel

    def classify_channel(self, channel, flag=True):
        if flag:  # have not execute preprocess
            channel = self.preprocess_channel(channel)

        if channel not in self.all_channels:
            return 'ERROR'
        if channel in classified_channels.keys():
            return classified_channels[channel]
        if re.search('购', channel):
            return '购物'
        if re.search('广播|电台|调频|频率|之声$|FM$|AM$', channel):
            return '电台'
        if re.search('试播|通道|试验|体验|测试|TEST|test', channel):
            return '其它'
        return '再分类'

    def preprocess_program(self, program):
        regexes = []
        chs_num = '一二三四五六七八九十'
        punctuations = env_punc + chs_punc
        unvisible_chars = ''.join([chr(i) for i in range(32)])
        regexes.append(re.compile('.*(报复|反复|回复|修复)$'))
        regexes.append(re.compile('(限免|中文版|英文版|回看|复播|重播|复|[上中下尾]|[ⅡⅢI]+)$'))
        regexes.append(re.compile('\s'))  # remove space chars
        regexes.append(re.compile('[%s]' % punctuations))  # remove punctuations
        regexes.append(re.compile('[%s]' % unvisible_chars))  # remove control chars
        regexes.append(re.compile('^(HD|3D)|(HD|SD|3D|TV|杜比)$'))  # remove program marks
        regexes.append(re.compile('(\d{2,4}年)*\d{1,2}月\d{1,2}日'))  # remove date
        regexes.append(re.compile('(第([%s]+|\d+)[部季集]+)$' % chs_num))  # remove serial number
        regexes.append(re.compile('(\d+|[%s]+)$' % chs_num))  # remove serial number

        for regex in regexes[2:]:
            program = re.sub(regex, '', program)
        if not re.match(regexes[0], program):
            program = re.sub(regexes[1], '', program)

        # remove serial number at the middle of the program name
        res = re.search('第([%s]+|\d+)[部集季]+' % chs_num, program)
        if res and not re.match('^\d+', program):
            program = program[:res.span()[0]]

        # remove serial number at the end of the program name again
        program = re.sub('(\d+|[%s]+)$' % chs_num, '', program)
        program = re.sub('(第([%s]+|\d+)[部季集]+)$' % chs_num, '', program)

        # remove chinese garbled
        if re.search('[^(\w+\-)]', program):
            return None

        return None if not program else program

    def classify_program(self, program):
        program = self.preprocess_program(program)

        if program is None:
            return '其它'
        # classify_program

    def normalize_scrapy_programs(self):
        """
        merge and normalize scrapy_programs by category
        :return:
        """

        os.chdir(SCRAPY_PATH)
        for category in ['电视剧', '电影', '动漫']:
            command = 'cat *_' + category + '.txt |sort|uniq > merge_scrapy_' + category + '.txt'
            os.system(command)

        for category in ['电视剧', '电影', '动漫']:
            src_path = SCRAPY_PATH + '/merge_scrapy_' + category + '.txt'
            des_path = SCRAPY_PATH + '/normalized_scrapy_' + category + '.txt'
            self.handler.normalize_programs(src_path, des_path)

    def get_reclassify_programs(self):
        """
        extract programms need to be reclassify
        :return: reclassify_programs.txt, reclassify_channel_programs.txt
        """

        with codecs.open(SCRAPY_PATH + '/normalized_scrapy_电影.txt', 'r') as fr:
            scrapy_movie_programs = set([line.strip() for line in fr.readlines()])
        with codecs.open(SCRAPY_PATH + '/normalized_scrapy_电视剧.txt', 'r') as fr:
            scrapy_tv_programs = set([line.strip() for line in fr.readlines()])
        with codecs.open(SCRAPY_PATH + '/normalized_scrapy_动漫.txt', 'r') as fr:
            scrapy_cartoon_programs = set([line.strip() for line in fr.readlines()])

        classifier = Classify()
        reclassify_programs = []
        reclassify_channel_programs = []
        all_files = sorted(os.listdir(EXTRACT_CHANNEL_PROGRAM))
        for file_name in all_files:
            if file_name == 'uniq_13.txt': continue
            if DEBUG: print('enter', file_name)
            fr_path = EXTRACT_CHANNEL_PROGRAM + '/' + file_name
            with codecs.open(fr_path, 'r', encoding='utf8') as fr:
                if file_name == 'uniq_96.txt':
                    programs = [line.strip() for line in set(fr.readlines()) if line.strip()]
                    for program in programs:
                        # 音乐、电视剧、体育
                        if re.match('^\d+-.*-.*$', program): continue
                        if re.search('^电视剧|剧场', program): continue
                        if re.search(sports_keywords, program): continue
                        res = classifier.preprocess_program(program)
                        if res: reclassify_programs.append(res)
                else:
                    for line in fr.readlines():
                        tmp = line.strip()
                        if not tmp: continue

                        res = tmp.split('|')
                        if len(res) != 2: continue
                        channel, program = res[0], res[1]

                        channel = classifier.preprocess_channel(channel)
                        program = classifier.preprocess_program(program)

                        if not program: continue
                        if re.search('^电视剧|剧场', program): continue
                        if re.search(sports_keywords, program): continue
                        if not channel:
                            reclassify_programs.append(program)
                            continue

                        category = classifier.classify_channel(channel, flag=False)
                        if category == '再分类':
                            reclassify_channel_programs.append(channel + '|' + program)
                            reclassify_programs.append(program)
                        else:
                            reclassify_programs.append(program)

        reclassify_programs = set(reclassify_programs)
        print(len(reclassify_programs))
        reclassify_programs -= scrapy_tv_programs
        print(len(reclassify_programs))
        reclassify_programs -= scrapy_movie_programs
        print(len(reclassify_programs))
        reclassify_programs -= scrapy_cartoon_programs
        print(len(reclassify_programs))

        regex = 'MV|金曲'
        music_programs = [program for program in reclassify_programs if re.search(regex, program)]
        reclassify_programs = [program for program in reclassify_programs if not re.search(regex, program)]
        print(len(reclassify_programs))

        regex = '|'.join([star.strip() for star in open(TMP_PATH + '/stars.txt', 'r').readlines()])
        star_programs = [program for program in reclassify_programs if re.search(regex, program)]
        reclassify_programs = [program for program in reclassify_programs if not re.search(regex, program)]
        print(len(reclassify_programs))

        with open(TMP_PATH + '/star_program.txt', 'w') as fw:
            fw.write('\n'.join(star_programs))

        with codecs.open(TMP_PATH + '/reclassify_programs.txt', 'w') as fw:
            fw.write('\n'.join(sorted(set(reclassify_programs))))
        with codecs.open(TMP_PATH + '/reclassify_channel_programs.txt', 'w') as fw:
            fw.write('\n'.join(sorted(set(reclassify_channel_programs))))

    def get_common_prefix(self, pre_str, cur_str, N=threshold):
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

    def search_common_prefix(self):
        with codecs.open(TMP_PATH + '/reclassify_programs.txt', 'r') as fr:
            programs = [line.strip() for line in fr.readlines()]

        # class getoutofloop(Exception): pass
        prefixs, extracted_programs = [], []

        pre, cur = 0, 1
        N, max_length = threshold, len(programs)
        while cur < max_length:
            pre_str, cur_str = programs[pre], programs[cur]
            prefix = self.get_common_prefix(pre_str, cur_str)
            if prefix != None:
                start_pos = pre
                pre, cur = cur, cur + 1
                while cur < max_length:
                    pre_str, cur_str = programs[pre], programs[cur]
                    res = self.get_common_prefix(pre_str, cur_str)
                    if res is None:
                        if not re.match('^\d+$', prefix[:4]):
                            prefixs.append(prefix)
                            extracted_programs.append(programs[start_pos: cur])
                        break
                    else:
                        prefix = res if len(res) < len(prefix) else prefix
                        pre, cur = cur, cur + 1
            pre, cur = cur, cur + 1

        handled_prefixs = []
        for prefix in prefixs:
            res = re.sub('\d+$', '', prefix)
            if len(res) < 2: print(prefix)
            handled_prefixs.append(res)

        with codecs.open(TMP_PATH + '/prefixs.txt', 'w') as fw:
            fw.write('\n'.join(sorted(set(prefixs))))

        with codecs.open(TMP_PATH + '/prefix_normalized.txt', 'w') as fw:
            fw.write('\n'.join(sorted(set(handled_prefixs))))

        with codecs.open(TMP_PATH + '/prefix_programs.txt', 'w') as fw:
            for prefix, programs in zip(prefixs, extracted_programs):
                fw.write('The prefix:' + prefix + '\n')
                fw.write('\n'.join(programs) + '\n\n')


if __name__ == '__main__':
    handler = Classify()
    # handler.classify_exist_channels('tmp_result/normalized_channels.txt')
    # handler.normalize_scrapy_programs()
    # handler.get_reclassify_programs()
    handler.search_common_prefix()
