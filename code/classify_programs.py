import re
import codecs
from string import punctuation as env_punc
from zhon.hanzi import punctuation as chs_punc
from basic_category import classified_channels


class Classify(object):
    def __init__(self):
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
        if flag: # have not execute preprocess
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
        regexes.append(re.compile('\s'))                            # remove space chars
        regexes.append(re.compile('[%s]' % punctuations))           # remove punctuations
        regexes.append(re.compile('[%s]' % unvisible_chars))        # remove control chars
        regexes.append(re.compile('^(HD|3D)|(HD|SD|3D|TV|杜比)$'))   # remove program marks
        regexes.append(re.compile('(\d{2,4}年)*\d{1,2}月\d{1,2}日'))       # remove date
        regexes.append(re.compile('(第([%s]+|\d+)[部季集]+)$' % chs_num))  # remove serial number
        regexes.append(re.compile('(\d+|[%s]+)$' % chs_num))        # remove serial number

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


if __name__ == '__main__':
    handler = Classify()
    handler.classify_exist_channels('tmp_result/normalized_channels.txt')
