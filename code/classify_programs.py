import os
import re
import codecs
from string import punctuation as env_punc
from zhon.hanzi import punctuation as chs_punc

from basic_category import classified_channels, sports_keywords, drama_keywords

DEBUG = True
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
        regexes.append(re.compile('现场直播|实况录像'))
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
        if re.search('购', channel) and channel != '时尚购物':
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
            '动画': '少儿',
            '天气预报': '生活',
            '歌曲': '音乐',
            '金曲': '音乐',
            'MV': '音乐',
            'mv': '音乐',
            '音乐榜': '音乐',
            '音乐流行榜': '音乐',
            '影院': '电影',
            '院线': '电影',
            '海军': '军事',
            '陆军': '军事',
            '空军': '军事',
            '二战': '二战',
            '零频道': '其它',
        }

        res = re.search(
            'MV|mv|金曲|音乐(流行)*榜|剧场|影院|院线|美食|纪录片|纪实|动漫|动画|天气预报|新闻|歌曲|零频道|海军|陆军|空军|二战|军事|军情|军旅|军营|军民|'
            '^(电视剧|电影|财经|凤凰|旅游|健康|健身|健美)', program)
        if res: return keymap[res.group()]

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
        classified_programs += [('2None', program, '电视剧') for program in intersection_1]
        unclassify_programs -= intersection_1

        intersection_2 = set.intersection(unclassify_programs, scrapy_movie_programs)
        classified_programs += [('2None', program, '电影') for program in intersection_2]
        unclassify_programs -= intersection_2

        intersection_3 = set.intersection(unclassify_programs, scrapy_cartoon_programs)
        classified_programs += [('2None', program, '少儿') for program in intersection_3]
        unclassify_programs -= intersection_3

        return classified_programs, unclassify_programs

    def classify_first(self):
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
            if re.match('^\d+-.*-.*$', item):
                classified_programs.append(('1None', program, '音乐'))
                continue

            category = self.classify_by_keywords(program)
            if not category:
                unclassify_programs.append(program)
            else:
                classified_programs.append(('1None', program, category))
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
            origin_program = program
            program = self.preprocess_program(program)
            if not program: continue
            if re.match('^\d+-.*-.*$', origin_program):
                classified_programs.append(('1None', program, '音乐'))
                continue

            # classify by keywords
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
        classified_programs = list(set(classified_programs))
        unclassify_programs = list(set(unclassify_programs))
        if DEBUG: print(len(classified_programs), len(unclassify_programs))

        programs = set([program for _, program, _ in classified_by_channel])
        unclassify_programs = list(set(unclassify_programs) - programs)
        classified_programs =list(set(classified_programs + classified_by_channel))
        if DEBUG: print(len(classified_programs), len(unclassify_programs))

        # classify by crawled programs
        classified_by_crawled, unclassify_programs = self.classify_by_crawled_programs(unclassify_programs)
        classified_programs = list(set(classified_programs + classified_by_crawled))
        if DEBUG: print(len(classified_programs), len(unclassify_programs))

        tmp_programs = [program for _, program, _ in classified_programs]
        reclassified, _ = self.classify_by_crawled_programs(tmp_programs)
        reclassified = dict([(program, category) for _, program, category in reclassified])
        for i in range(len(classified_programs)):
            program = classified_programs[i][1]
            if program in list(reclassified.keys()):
                classified_programs[i] = ('2None', program, reclassified[program])
        classified_programs = set(classified_programs)
        if DEBUG: print(len(classified_programs), len(unclassify_programs))

        print(len(set([(program, category) for _, program, category in classified_programs])))
        print(len(set([program for _, program, _ in classified_programs])))

        classified_programs = sorted(classified_programs, key=lambda item: item[0])
        with codecs.open(TMP_PATH + '/reclassify_programs.txt', 'w') as fw:
            fw.write('\n'.join(sorted(unclassify_programs)))
        with codecs.open(TMP_PATH + '/all_programs_category.txt', 'w') as fw:
            fw.write('\n'.join(['%s\t\t%s\t\t%s' % (a, c, b) for a, b, c in classified_programs]))

        return classified_programs, unclassify_programs

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

    def classify_by_xingchen(self):
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

    def merge_classify_prefix(self, classified_programs, unclassify_programs):
        unclassify_programs = set(unclassify_programs)
        prefix_classified = self.classify_by_xingchen()
        classified_programs += prefix_classified
        unclassify_programs -= set([program for _, program, _ in prefix_classified])
        classified_programs = set(classified_programs)

        if DEBUG: print(len(classified_programs), len(unclassify_programs))
        print(len(set([(program, category) for _, program, category in classified_programs])))
        print(len(set([program for _, program, _ in classified_programs])))

        classified_programs = sorted(classified_programs, key=lambda item: (item[0], item[2], items[3]))
        with codecs.open(TMP_PATH + '/reclassify_programs_2.txt', 'w') as fw:
            fw.write('\n'.join(sorted(unclassify_programs)))
        with codecs.open(TMP_PATH + '/all_programs_category_2.txt', 'w') as fw:
            fw.write('\n'.join(['%s\t\t%s\t\t%s' % (a, c, b) for a, b, c in classified_programs]))


if __name__ == '__main__':
    handler = Classifyer()
    # handler.classify_exist_channels('tmp_result/normalized_channels.txt')
    # handler.normalize_scrapy_programs()
    # handler.classify_first()
    # handler.search_common_prefix()

    with open(TMP_PATH + '/all_programs_category.txt', 'r') as fr:
        items = [line.strip() for line in fr.readlines()]
        classified_programs = [item.split('\t\t') for item in items]
        classified_programs = [(a, c, b) for a, b, c in classified_programs]
    with open(TMP_PATH + '/reclassify_programs.txt', 'r') as fr:
        unclassify_programs = [line.strip() for line in fr.readlines()]

    handler.merge_classify_prefix(classified_programs, unclassify_programs)
