import re
import codecs
from bs4 import BeautifulSoup
from urllib.request import urlopen, quote

categories = ['电影', '电视剧', '综艺', '新闻', '体育',
              '财经', '法治', '军事', '农业', '纪实',
              '科教', '音乐', '戏曲', '少儿', '健康',
              '时尚', '美食', '汽车', '旅游', '生活',  # life is in the later
              '亲子', '购物', '电台', '其它']

class Classify(object):
    def __init__(self):
        pass

    def classify_channels(self, file_path):
        """
        classify the channels
        :param file_path:path the target file
        """

        with codecs.open(file_path, 'r', encoding='utf8') as fr:
            regexs = ['^(CCTV|中央)', '卫视$', '广播|电台|调频|频率|之声$|FM$|AM$', '试播|通道|试验|体验|测试|TEST|test', '新闻|资讯', '购物', '音乐', '影院|影视|电影', '\w+']

            channels = [line.strip() for line in fr]
            categories = [[] for _ in range(len(regexs) + 1)]
            for channel in channels:
                for i in range(len(regexs)):
                    if re.search(regexs[i], channel, re.I):
                        categories[i].append(channel)
                        break

            with codecs.open("tmp_result/2classified_channels.txt", 'w', encoding='utf8') as fw:
                for i in range(len(categories)):
                    fw.write('\n'.join(sorted(set(categories[i]))))
                    fw.write('\n' * 2)

    def scrapy_programs(self, program):
        """
        爬取百度百科节目简介
        :param program:节目名称
        :return:简介内容或error
        """

        url = 'https://baike.baidu.com/item/' + quote(program)
        html = urlopen(url)
        bsObj = BeautifulSoup(html, 'html.parser')
        if bsObj.head.title.get_text() == '百度百科——全球最大中文百科全书':
            return 'error'
        return bsObj.head.find_all('meta')

if __name__ == '__main__':
    handler = Classify()
    # handler.classify_channels('tmp_result/normalized_channels.txt')
    handler.classify_channels('tmp_result/2normalized_channels.txt')
