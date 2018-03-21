"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author:Gang Zhang
  Date:2018.02.01

  Function:
    proxy manage for crawling
"""

from redis import Redis
from random import choice
from redis.connection import BlockingConnectionPool


class ProxyPool(object):
    def __init__(self):
        self.headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/'
                    '537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}

    def get_proxy(self):
        """
        get a new proxy from proxy pool
        :return:
        """

        session = Redis(connection_pool=BlockingConnectionPool(host='localhost', port=6379))
        while True:
            try:
                proxies = list(session.hgetall('useful_proxy').keys())
                new_proxy = choice(proxies).decode('utf8')
                break
            except:
                pass
        return new_proxy

    def delete_proxy(self, proxy):
        """
        delete the given proxy from proxy pool
        :param proxy:
        :return:
        """

        session = Redis(connection_pool=BlockingConnectionPool(host='localhost', port=6379))
        session.hdel('useful_proxy', proxy)

    def change_proxy(self):
        """
        change current proxy
        :return:
        """

        # from urllib.request import ProxyHandler, build_opener, install_opener
        # if self.proxy:
        #     self.delete_proxy(self.proxy)
        # self.proxy = self.get_proxy()
        # proxy_support = ProxyHandler({'http': self.proxy})
        # opener = build_opener(proxy_support)
        # opener.addheaders = [('User-Agent', self.headers['User-Agent'])]
        # install_opener(opener)


if __name__ == '__main__':
    pass
