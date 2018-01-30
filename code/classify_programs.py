import os
import re
import codecs
from string import punctuation as env_punc
from zhon.hanzi import punctuation as chs_punc

from basic_category import classified_channels, sports_keywords, drama_keywords

DEBUG = True
threshold = 4
TMP_PATH = os.getcwd() + '/tmp_result'
SCRAPY_PATH = TMP_PATH + '/scrapy_programs'
EXTRACT_CHANNEL_PROGRAM = TMP_PATH + '/extract_channel_program'


class Classifyer(object):
    def __init__(self):
        with codecs.open('BA_all_channels.txt', 'r', encoding='utf8') as fr:
            self.all_channels = [line.strip() for line in fr.readlines()]

    def regex_for_normalize_programs(self):
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
        regexes.append(re.compile('(中文版|英文版|回看|复播|重播|[上中下尾]|[ⅡⅢI]+)$'))
        regexes.append(re.compile('^(HD|3D|杜比)|(SD文广版|文广(遮标)*版|SD|HD|3D|TV|杜比|限免|done|out)$'))

        # remove space/punctuations/control chars
        regexes.append(re.compile('\s'))
        regexes.append(re.compile('[%s]' % punctuations))
        regexes.append(re.compile('[%s]' % unvisible_chars))

        # remove date
        regexes.append(re.compile('(19|20)[0-9]{2}[年]*\d*'))
        regexes.append(re.compile('(\d{2,4}年)*\d{1,2}月\d{1,2}日'))
        regexes.append(re.compile('\d*(0[0-9]|1[0-2])([0-2][0-9]|3[0-1])\d*'))

        # remove serial number
        regexes.append(re.compile('(第*([%s]+|\d+)[期部季集]+)$' % chs_num))
        regexes.append(re.compile('(\d+|[%s]+)$' % chs_num))

        return regexes

    def preprocess_channel(self, channel):
        """
        preprocess of channel
        :param channel:
        :return:
        """

        # remove punctuations
        punctuations = env_punc + chs_punc
        channel = re.sub('[%s]' % punctuations, '', channel)

        # channel includes chinese garbled or is purely made up with number
        if re.search('[^(\w+\-)]', channel) or re.match('^[0-9]+$', channel):
            return None

        # remove Dolby、HD、高清 channels
        if len(channel) >= 5 and re.search('(杜比|高清|频道|HD)$', channel):
            channel = channel[:-2]
        elif re.search('Dolby$', channel):
            channel = channel[:-5]
        elif re.search('^NVOD', channel) and channel != 'NVOD4K':
            channel = channel[4:]
        return channel

    def preprocess_program(self, program):
        """
        preprocess of program
        :param program:
        :return:
        """

        chs_num = '一二三四五六七八九十'
        regexes = self.regex_for_normalize_programs()

        for regex in regexes[2:]:
            program = re.sub(regex, '', program)
        if not re.match(regexes[0], program):
            program = re.sub(regexes[1], '', program)

        for regex in regexes[2:]:
            program = re.sub(regex, '', program)
        if not re.match(regexes[0], program):
            program = re.sub(regexes[1], '', program)

        # remove serial number in the middle of the program name
        if re.match('^\D+第([%s]+|\d+)[部集季]+.*$' % chs_num, program):
            res = re.search('第([%s]+|\d+)[部集季]+' % chs_num, program)
            program = program[:res.span()[0]]

        # remove chinese garbled
        if re.search('[^(\w+\-)]', program):
            return None

        return None if not program else program

    def classify_by_channel(self, channel):
        """
        classify program by channels
        :param channel:
        :return:
        """

        if channel not in self.all_channels:
            return 'ERROR'
        if channel in list(classified_channels.keys()):
            return classified_channels[channel]
        if re.search('购', channel):
            return '购物'
        if re.search('广播|电台|调频|频率|之声$|FM$|AM$', channel):
            return '电台'
        if re.search('通道|试验|体验|测试|TEST|test', channel):
            return '其它'
        return '再分类'

    def classify_by_keywords(self, program):
        """
        classify program by keywords
        :param program:
        :return:
        """

        keymap = {
            '美食': '美食',
            '电影': '电影',
            '财经': '财经',
            '旅游': '旅游',
            '剧场': '电视剧',
            '电视剧': '电视剧',
            '新闻': '新闻',
            '凤凰': '新闻',
            '纪录片': '纪实',
            '纪实': '纪实',
            '军事': '军事',
            '军情': '军事',
            '军旅': '军事',
            '军营': '军事',
            '军民': '军事',
            '健康': '健康',
            '健身': '健康',
            '健美': '健康',
            '动漫': '少儿',
            '天气预报': '生活',
            '歌曲': '音乐',
            '金曲':'音乐',
            'MV':'音乐',
            'mv':'音乐',
        }

        res = re.search(
            'MV|mv|金曲|剧场|美食|纪录片|纪实|动漫|天气预报|新闻|歌曲|军事|军情|军旅|军营|军民|'
            '^(电视剧|电影|财经|凤凰|旅游|健康|健身|健美)', program)
        if res: return keymap[res.group()]

        if re.match('^\d+-.*-.*$', program):
            return "音乐"
        if re.search(sports_keywords, program):
            return "体育"
        if re.search(drama_keywords, program):
            return "戏曲"

        file_path = TMP_PATH + '/keyword_entertainment.txt'
        regex = '|'.join([star.strip() for star in open(file_path, 'r').readlines()])
        if re.search(regex, program):
            return '综艺'
        return None

    def classify_by_crawled_programs(self, unclassify_programs):
        """
        classify program by programs crawled from tv websites
        :param unclassify_programs:
        :return:
        """

        with codecs.open(SCRAPY_PATH + '/normalized_scrapy_电影.txt', 'r') as fr:
            scrapy_movie_programs = set([line.strip() for line in fr.readlines()])
        with codecs.open(SCRAPY_PATH + '/normalized_scrapy_电视剧.txt', 'r') as fr:
            scrapy_tv_programs = set([line.strip() for line in fr.readlines()])
        with codecs.open(SCRAPY_PATH + '/normalized_scrapy_动漫.txt', 'r') as fr:
            scrapy_cartoon_programs = set([line.strip() for line in fr.readlines()])

        # 需要改进匹配算法
        classified_programs = []
        unclassify_programs = set(unclassify_programs)

        intersection_1 = set.intersection(unclassify_programs, scrapy_tv_programs)
        classified_programs += [('1None', program, '电视剧') for program in intersection_1]
        unclassify_programs -= intersection_1

        intersection_2 = set.intersection(unclassify_programs, scrapy_movie_programs)
        classified_programs += [('1None', program, '电影') for program in intersection_2]
        unclassify_programs -= intersection_2

        intersection_3 = set.intersection(unclassify_programs, scrapy_cartoon_programs)
        classified_programs += [('1None', program, '少儿') for program in intersection_3]
        unclassify_programs -= intersection_3

        return classified_programs, unclassify_programs

    def classify_all_programs(self):
        """
        classify all programs for the first step
        :return:
        """

        all_programs = []
        all_channel_programs = []
        all_files = sorted(os.listdir(EXTRACT_CHANNEL_PROGRAM))
        for file_name in all_files:
            if file_name == 'uniq_13.txt': continue
            fr_path = EXTRACT_CHANNEL_PROGRAM + '/' + file_name
            with codecs.open(fr_path, 'r', encoding='utf8') as fr:
                if file_name == 'uniq_96.txt':
                    all_programs += [line.strip() for line in set(fr.readlines()) if line.strip()]
                else:
                    all_channel_programs += [line.strip() for line in set(fr.readlines()) if line.strip()]

        classified_programs = []
        unclassify_programs = []

        # classify programs without channel
        for item in set(all_programs):
            program = self.preprocess_program(item)
            if not program: continue
            category = self.classify_by_keywords(program)
            if not category:
                unclassify_programs.append(program)
            else:
                classified_programs.append(('1None', program, category))

        # 1.print debug info -- classified by keywords
        classified_programs = list(set(classified_programs))
        unclassify_programs = list(set(unclassify_programs))
        if DEBUG: print(len(classified_programs), len(unclassify_programs))

        # classify programs with channel
        classified_by_channel = []
        for item in set(all_channel_programs):
            if not item: continue
            res = item.split('|')
            if len(res) != 2: continue
            channel, program = res
            if not program: continue

            channel = self.preprocess_channel(channel)
            program = self.preprocess_program(program)

            # classify by keywords
            if not program: continue
            category = self.classify_by_keywords(program)
            if category:
                classified_programs.append(('1None', program, category))
                continue

            # classify by channel
            if not channel: unclassify_programs.append(program)
            category = self.classify_by_channel(channel)
            if not re.search('再分类|ERROR', category) and program:
                classified_by_channel.append((channel, program, category))
                continue

            # can not be classified
            unclassify_programs.append(program)

        # 2.print debug info -- classified by keywords
        classified_programs = list(set(classified_programs))
        unclassify_programs = list(set(unclassify_programs))
        if DEBUG: print(len(classified_programs), len(unclassify_programs))

        programs = set([program for _, program, _ in classified_by_channel])
        unclassify_programs = list(set(unclassify_programs) - programs)
        classified_programs =list(set(classified_programs + classified_by_channel))

        # 3.print debug info -- classified by channel
        classified_programs = list(set(classified_programs))
        unclassify_programs = list(set(unclassify_programs))
        if DEBUG: print(len(classified_programs), len(unclassify_programs))

        # classify by crawled programs
        classified_by_crawled, unclassify_programs = self.classify_by_crawled_programs(unclassify_programs)
        classified_programs = list(set(classified_programs + classified_by_crawled))

        # 4.print debug info -- classified by crawled
        classified_programs = list(set(classified_programs))
        unclassify_programs = list(set(unclassify_programs))
        if DEBUG: print(len(classified_programs), len(unclassify_programs))

        print(len(classified_programs))
        print(len(set([(program, category) for _, program, category in classified_programs])))
        print(len(set([program for _, program, _ in classified_programs])))

        classified_programs = sorted(classified_programs, key=lambda item: item[0])

        # write unclassify programs into file
        with codecs.open(TMP_PATH + '/reclassify_programs.txt', 'w') as fw:
            fw.write('\n'.join(sorted(set(unclassify_programs))))

        # write classifed programs into file
        with codecs.open(TMP_PATH + '/all_programs_category.txt', 'w') as fw:
            fw.write('\n'.join(['%s\t\t%s\t\t%s' % (a, b, c) for a, b, c in classified_programs]))

    def classify_all_programs_dev(self):
        all_programs = []
        all_channel_programs = []
        all_files = sorted(os.listdir(EXTRACT_CHANNEL_PROGRAM))
        for file_name in all_files:
            if file_name == 'uniq_13.txt': continue
            fr_path = EXTRACT_CHANNEL_PROGRAM + '/' + file_name
            with codecs.open(fr_path, 'r', encoding='utf8') as fr:
                if file_name == 'uniq_96.txt':
                    all_programs += [line.strip() for line in set(fr.readlines()) if line.strip()]
                else:
                    all_channel_programs += [line.strip() for line in set(fr.readlines()) if line.strip()]

        for item in all_channel_programs:
            if not item: continue
            res = item.split('|')
            if len(res) != 2: continue
            channel, program = res
            if not program: continue
            all_programs.append(program)


        all_programs = set([self.preprocess_program(program) for program in set(all_programs)])
        all_programs = set([program for program in all_programs if program])
        classified_programs, unclassify_programs = self.classify_by_crawled_programs(all_programs)
        for program in unclassify_programs:
            category = self.classify_by_keywords(program)
            if category:
                classified_programs.append(('1None', program, category))

        unclassify_programs = set(unclassify_programs) - set(classified_programs)
        classified_programs = list(set(classified_programs))
        print(len(classified_programs), len(unclassify_programs))

        for item in set(all_channel_programs):
            if not item: continue
            res = item.split('|')
            if len(res) != 2: continue
            channel, program = res
            if not program: continue

            program = self.preprocess_program(program)
            if not program: continue
            if program in classified_programs: continue

            channel = self.preprocess_channel(channel)
            if not channel: continue

            category = self.classify_by_channel(channel)
            if not re.search('再分类|ERROR', category) and program:
                classified_programs.append((channel, program, category))
                continue

        classified_programs = set(classified_programs)
        unclassify_programs = set(unclassify_programs) - classified_programs
        print(len(classified_programs), len(unclassify_programs))

        print(len(classified_programs), len(set([program for _, program, _ in classified_programs])))
        classified_programs = sorted(classified_programs, key=lambda item: item[1])

        # write unclassify programs into file
        with codecs.open(TMP_PATH + '/reclassify_programs.txt', 'w') as fw:
            fw.write('\n'.join(sorted(set(unclassify_programs))))

        # write classifed programs into file
        with codecs.open(TMP_PATH + '/all_programs_category.txt', 'w') as fw:
            fw.write('\n'.join(['%s\t\t%s\t\t%s' % (a, b, c) for a, b, c in classified_programs]))

    def get_common_prefix(self, pre_str, cur_str, N=threshold):
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
            if prefix:
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

        with codecs.open(TMP_PATH + '/prefix_programs.txt', 'w') as fw:
            fw.write('\n'.join(sorted(set(prefixs))))

        with codecs.open(TMP_PATH + '/prefix_normalized.txt', 'w') as fw:
            fw.write('\n'.join(sorted(set(handled_prefixs))))

        count = 0
        with codecs.open(TMP_PATH + '/prefix_lists.txt', 'w') as fw:
            for prefix, programs in zip(prefixs, extracted_programs):
                count += len(programs)
                fw.write('The prefix:' + prefix + '\n')
                fw.write('\n'.join(programs) + '\n\n')
        print(count)


if __name__ == '__main__':
    handler = Classifyer()
    # handler.classify_exist_channels('tmp_result/normalized_channels.txt')
    # handler.normalize_scrapy_programs()
    handler.classify_all_programs()
    handler.search_common_prefix()
