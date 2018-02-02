"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author:Gang Zhang
  Date:2018.02.01
"""

import re, time
from lxml import etree
from web_request import WebRequest

def getHtmlTree(url):
    """
    get html tree
    :param url:
    :return:
    """

    header = {'Connection': 'keep-alive',
              'Cache-Control': 'max-age=0',
              'Upgrade-Insecure-Requests': '1',
              'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko)',
              'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
              'Accept-Encoding': 'gzip, deflate, sdch',
              'Accept-Language': 'zh-CN,zh;q=0.8',
              }

    wr = WebRequest()
    time.sleep(2)
    html = wr.get(url=url, header=header).content
    return etree.HTML(html)


class GetFreeProxy(object):
    """
    crawl free proxies
    """

    def __init__(self):
        pass

    @staticmethod
    def freeProxyFirst():
        """
        crawl 5u proxy http://www.data5u.com/
        :return:
        """

        url_list = ['http://www.data5u.com/',
                    'http://www.data5u.com/free/',
                    'http://www.data5u.com/free/gngn/index.shtml',
                    'http://www.data5u.com/free/gnpt/index.shtml']

        for url in url_list:
            html_tree = getHtmlTree(url)
            ul_list = html_tree.xpath('//ul[@class="l2"]')
            for ul in ul_list:
                try:
                    yield ':'.join(ul.xpath('.//li/text()')[0:2])
                except:
                    pass

    @staticmethod
    def freeProxySecond(proxy_number=100):
        """
        crawl 66 proxy http://www.66ip.cn/
        :param proxy_number: number of the proxies
        :return:
        """

        url = "http://www.66ip.cn/mo.php?sxb=&tqsl={}&port=&export=&" \
              "ktip=&sxa=&submit=%CC%E1++%C8%A1&textarea=".format(proxy_number)

        request = WebRequest()
        html = request.get(url).text # decoded string
        for proxy in re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}', html):
            yield proxy

    @staticmethod
    def freeProxyThird():
        """
        crawl ip181 proxy http://www.ip181.com/
        :return:
        """

        url = 'http://www.ip181.com/'

        html_tree = getHtmlTree(url)
        try:
            tr_list = html_tree.xpath('//tr')[1:]
            for tr in tr_list:
                yield ':'.join(tr.xpath('./td/text()')[0:2])
        except:
            pass

    @staticmethod
    def freeProxyFourth():
        """
        crawl xici proxy http://api.xicidaili.com/free2016.txt
        :return:
        """

        url_list = ['http://www.xicidaili.com/nn',  # 高匿
                    'http://www.xicidaili.com/nt',] # 透明

        for each_url in url_list:
            tree = getHtmlTree(each_url)
            proxy_list = tree.xpath('.//table[@id="ip_list"]//tr')
            for proxy in proxy_list:
                try:
                    yield ':'.join(proxy.xpath('./td/text()')[0:2])
                except:
                    pass

    @staticmethod
    def freeProxyFifth():
        """
        crawl guobanjia proxy http://www.goubanjia.com/free/gngn/index.shtml
        :return:
        """

        url = "http://www.goubanjia.com/free/gngn/index{page}.shtml"
        for page in range(1, 10):
            page_url = url.format(page=page)
            tree = getHtmlTree(page_url)
            proxy_list = tree.xpath('//td[@class="ip"]')
            xpath_str = """
                      .//*[not(contains(@style, 'display: none'))
                      and not(contains(@style, 'display:none'))
                      and not(contains(@class, 'port'))]/text()
                      """
            for each_proxy in proxy_list:
                try:
                    ip_addr = ''.join(each_proxy.xpath(xpath_str))
                    port = each_proxy.xpath(".//span[contains(@class, 'port')]/text()")[0]
                    yield '{}:{}'.format(ip_addr, port)
                except:
                    pass

    @staticmethod
    def freeProxySixth():
        """
        crawl xun proxy http://www.xdaili.cn/ipagent/freeip/getFreeIps?page=1&rows=10
        :return:
        """

        url = 'http://www.xdaili.cn/ipagent/freeip/getFreeIps?page=1&rows=10'

        request = WebRequest()
        try:
            res = request.get(url).json()
            for row in res['RESULT']['rows']:
                yield '{}:{}'.format(row['ip'], row['port'])
        except:
            pass

    @staticmethod
    def freeProxySeventh():
        """
        crawl kuai proxy https://www.kuaidaili.com/free/inha/1/
        """

        url = 'https://www.kuaidaili.com/free/inha/{page}/'

        for page in range(1, 10):
            page_url = url.format(page=page)
            tree = getHtmlTree(page_url)
            proxy_list = tree.xpath('.//table//tr')
            for tr in proxy_list[1:]:
                yield ':'.join(tr.xpath('./td/text()')[0:2])


if __name__ == '__main__':
    gg = GetFreeProxy()

    for e in gg.freeProxyFirst(): print(e)
    for e in gg.freeProxySecond(): print(e)
    for e in gg.freeProxyThird(): print(e)
    for e in gg.freeProxyFourth(): print(e)
    for e in gg.freeProxyFifth(): print(e)
    for e in gg.freeProxySixth(): print(e)
    for e in gg.freeProxySeventh(): print(e)
