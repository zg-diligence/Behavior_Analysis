import re
import codecs
from bs4 import BeautifulSoup
from urllib.request import urlopen, quote

class Classify(object):
    def __init__(self):
        pass

    def classify_channels(self, file_path):
        """
        对频道进行大致分类
        :param file_path:文件路径
        """

        with codecs.open(file_path, 'r', encoding='utf8') as fr:
            regexs = ['^CCTV', '卫视$', '广播|电台|FM$|AM$', '新闻|资讯', '购物', '音乐', '影院|影视|电影', '\w+']
            channels = [line.strip() for line in fr]
            categories = [[] for _ in range(len(regexs) + 1)]
            for channel in channels:
                for i in range(len(regexs)):
                    if re.search(regexs[i], channel, re.I):
                        categories[i].append(channel)
                        break
            with codecs.open("tmp_result/classified_channels.txt", 'w', encoding='utf8') as fw:
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
    pass
