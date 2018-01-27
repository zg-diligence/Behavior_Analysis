import os
import codecs
from time import sleep
from random import randint
from bs4 import BeautifulSoup
from urllib.request import urlopen, quote, Request

DEBUG = True
TMP_PATH = os.getcwd() + '/tmp_result'

class Scrapyer(object):
     def __init__(self):
         pass

     def search_program_info(self, program):
        headers = {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/'
                '537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}

        proxies = {

        }
        # headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0'}

        url = 'http://www.tvmao.com/query.jsp?keys=%s&ed='  % quote(program) + \
           'bOWkp%2BeZveWkq%2BWmh%2BS4iua8lOazoeayq%2BS5i%2BWQu28%3D'

        try:
            html = urlopen(Request(url=url, headers=headers))
            bsObj = BeautifulSoup(html, 'html.parser')

            page_content = bsObj.find_all('div', class_='page-content')[0]
            page_columns = [item.a.get_text() for item in page_content.dl.find_all('dd')]

            print(page_columns)

            page_content_uls = page_content.div.find_all('ul')

            if len(page_columns) == 0:
                return 'not_exist'

        except Exception as e:
            print('error =>', e)


if __name__ == '__main__':
    handler = Scrapyer()

    # programs = ['一站', '一站到底', '一线城市', '一起来画画', '一起玩游戏', '一起音乐吧']

    with codecs.open(TMP_PATH + '/prefix_normalized.txt', 'r') as fr:
        programs = [line.strip() for line in fr.readlines()]

    for program in set(programs):
        print('\n', program)
        handler.search_program_info(program)
        sleep(randint(2, 5))