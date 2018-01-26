import re
import codecs
from bs4 import BeautifulSoup
from urllib.request import urlopen, quote

categories = ['电影', '电视剧', '综艺', '新闻', '体育',
              '财经', '法治',   '军事', '农业', '纪实',
              '科教', '音乐',   '戏曲', '少儿', '健康',
              '时尚', '美食',   '汽车', '旅游', '生活',  # life is in the later
              '亲子', '购物',   '电台', '其它']

class Classify(object):
    def __init__(self):
        pass

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

    def scrapy_programs(self, program):
        url = 'https://baike.baidu.com/item/' + quote(program)
        html = urlopen(url)
        bsObj = BeautifulSoup(html, 'html.parser')
        if bsObj.head.title.get_text() == '百度百科——全球最大中文百科全书':
            return 'error'
        return bsObj.head.find_all('meta')

    def preprocess_channel(self, channel):
        pass

    def preprocess_program(self, program):
        pass

    def classify_channel(self, channel):
        pass

    def classify_program(self, program):
        pass

if __name__ == '__main__':
    handler = Classify()
    # handler.classify_exist_channels('tmp_result/normalized_channels.txt')
    handler.classify_exist_channels('tmp_result/original_handled_channels.txt')
