import os, re
import time, codecs
from multiprocessing import Pool
from string import punctuation as env_punc
from zhon.hanzi import punctuation as chs_punc

from program_third_classifyer import DistanceClassifyer
from basic_category import classified_channels, sports_keywords, \
    drama_keywords, education_keywords, music_keywords, military_keywords

DEBUG = True
TMP_PATH = os.getcwd() + '/tmp_result'
SCRAPY_PATH = TMP_PATH + '/scrapy_programs'
EXTRACT_CHANNEL_PROGRAM = TMP_PATH + '/extract_channel_program'


class Classifyer(object):
    def __init__(self):
        with codecs.open(TMP_PATH + '/BA_all_channels.txt', 'r', encoding='utf8') as fr:
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
            '院线': '电影',
            '影院': '电影',
            '财经': '财经',
            '旅游': '旅游',
            '剧场': '电视剧',
            '电视剧': '电视剧',
            '新闻': '新闻',
            '凤凰': '新闻',
            '纪录片': '纪实',
            '纪实': '纪实',
            '健康': '健康',
            '健身': '健康',
            '健美': '健康',
            '动漫': '少儿',
            '动画': '少儿',
            '儿歌': '少儿',
            '天气': '生活',
            '零频道': '其它',
            }

        if re.search(sports_keywords, program):
            return "体育"
        if re.search(drama_keywords, program):
            return "戏曲"
        if re.search(education_keywords, program):
            return '科教'
        if re.search(music_keywords, program):
            return '音乐'
        if re.search(military_keywords, program):
            return '军事'

        res = re.search(
            '剧场|影院|院线|美食|纪录片|纪实|动漫|动画|天气|新闻|儿歌|零频道|'
            '^(电视剧|电影|财经|凤凰|旅游|健康|健身|健美)', program)
        if res: return keymap[res.group()]

        file_path = TMP_PATH + '/keyword_entertainment.txt'
        regex = '|'.join([star.strip() for star in open(file_path, 'r').readlines()])
        if re.search(regex, program):
            return '综艺'
        return None

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
        with codecs.open(SCRAPY_PATH + '/normalized_scrapy_电影.txt', 'r') as fr:
            all_programs.append([line.strip() for line in fr.readlines()])

        categories = ['zongyi', 'tiyu', 'news', 'cai', 'junshi', 'lvyou', 'shaoer', 'fazhi']
        for category in categories:
            with codecs.open(SCRAPY_PATH + '/dianshiyan_' + category + '.txt', 'r') as fr:
                all_programs.append([line.strip() for line in fr.readlines()])
        return all_programs

    def classify_by_gold_programs(self, program, all_programs):
        """
        classify program by programs crawled from tv websites
        :param program:
        :param all_programs
        :return:
        """

        correct_categories = ['电视剧', '纪实', '少儿', '电影', '综艺', '体育', '新闻',
                              '财经', '军事', '旅游', '少儿', '法制']

        handler = DistanceClassifyer()
        min_programs, min_distances = [], []
        for i in range(len(all_programs)):
            item, distance = handler.find_min_distance(program, all_programs[i])
            if distance == 1.0: return correct_categories[i]
            min_programs.append(item)
            min_distances.append(distance)

        min_distance = max(min_distances)
        if min_distance > 0.93:
            return correct_categories[min_distances.index(min_distance)]
        return None

    def classify_programs(self, programs):
        """
        classify programs without channel
        :param programs:
        :return:
        """

        classified_programs = []
        unclassify_programs = []

        count = 0
        programs = list(set(programs))
        gold_programs = self.read_gold_programs()
        for program in programs:
            count += 1
            if count % 500 == 0:
                rate = int(count/len(programs)*100)
                if DEBUG: print(os.getpid(), '=>', '%d%%'%rate)

            # classified by crawled programs
            category = self.classify_by_gold_programs(program, gold_programs)
            if category:
                classified_programs.append(('2None', program, category))
                continue

            # classify by keywords
            category = self.classify_by_keywords(program)
            if category:
                classified_programs.append(('1None', program, category))
                continue

            # can not be classified
            unclassify_programs.append(program)
        return classified_programs, unclassify_programs

    def classify_channel_programs(self, channel_programs):
        """
        classify programs with channel
        :param channel_programs:
        :return:
        """

        classified_by_channel = []
        channel_programs = set(channel_programs)
        for program, channel in set(channel_programs):
            if not channel: continue
            category = self.classify_by_channel(channel)
            if not re.search('再分类|ERROR', category):
                classified_by_channel.append((channel, program, category))
                continue
        return classified_by_channel

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

        channel_programs = []
        classified_programs = []
        unclassify_programs = []

        # extract all programs
        for item in set(all_channel_programs):
            if not item: continue
            res = item.split('|')
            if len(res) != 2: continue
            channel, program = res
            if not program: continue

            # music
            if re.match('^\d+-.*-.*$', item):
                classified_programs.append(('1None', program, '音乐'))
                continue

            program = self.preprocess_program(program)
            if not program: continue
            channel = self.preprocess_channel(channel)
            unclassify_programs.append(program)
            channel_programs.append((program, channel))

        for item in set(all_programs):
            program = self.preprocess_program(item)
            if not program: continue

            # music
            if re.match('^\d+-.*-.*$', item):
                classified_programs.append(('1None', program, '音乐'))
                continue
            unclassify_programs.append(program)

        channel_programs = list(set(channel_programs))
        classified_programs = list(set(classified_programs))
        unclassify_programs = list(set(unclassify_programs))
        if DEBUG: print(len(classified_programs), len(unclassify_programs))
        if DEBUG: print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

        pool = Pool(4)
        processes = []
        N = len(unclassify_programs) // 4 + 1
        for i in range(4):
            items = unclassify_programs[i*N: i*N+N]
            processes.append(pool.apply_async(self.classify_programs, (items, )))
        pool.close()
        pool.join()

        unclassify_programs = []
        for p in processes:
            res = p.get()
            classified_programs += res[0]
            unclassify_programs += res[1]

        classified_programs = list(set(classified_programs))
        unclassify_programs = list(set(unclassify_programs))
        if DEBUG: print(len(classified_programs), len(unclassify_programs))
        if DEBUG: print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

        gold_programs = [program for _, program, _ in classified_programs]
        channel_programs = [item for item in channel_programs if item[0] not in gold_programs]

        pool = Pool(4)
        processes = []
        N = len(channel_programs) // 4 + 1
        for i in range(4):
            items = channel_programs[i * N: i * N + N]
            processes.append(pool.apply_async(self.classify_channel_programs, (items,)))
        pool.close()
        pool.join()

        classified_by_channel = []
        for p in processes:
            res = p.get()
            classified_by_channel += res

        programs = set([program for _, program, _ in classified_by_channel])
        unclassify_programs = list(set(unclassify_programs) - programs)
        classified_programs =list(set(classified_programs + classified_by_channel))
        if DEBUG: print(len(classified_programs), len(unclassify_programs))

        print(len(set([(program, category) for _, program, category in classified_programs])))
        print(len(set([program for _, program, _ in classified_programs])))

        classified_programs = sorted(classified_programs, key=lambda item: (item[0], item[2], item[1]))
        with codecs.open(TMP_PATH + '/reclassify_programs_1.txt', 'w') as fw:
            fw.write('\n'.join(sorted(unclassify_programs)))
        with codecs.open(TMP_PATH + '/all_programs_category_1.txt', 'w') as fw:
            fw.write('\n'.join(['%s\t\t%s\t\t%s' % (a, c, b) for a, b, c in classified_programs]))

        return classified_programs, unclassify_programs


if __name__ == '__main__':
    handler = Classifyer()
    handler.classify_first()
