"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author:Gang Zhang
  Date:2018.02.01
"""

import random
import requests
from redis_client import RedisClient
from get_free_proxy import GetFreeProxy

proxy_getter_methods = ['freeProxyFirst',
        'freeProxySecond', 'freeProxyThird',
        'freeProxyFourth', 'freeProxyFifth',
        'freeProxySixth', 'freeProxySeventh']

class ProxyManager(object):
    """
    manager of the proxy pool
    """

    def __init__(self, logger):
        self.db = RedisClient(name='raw_proxy', host='localhost', port=6379)
        self.raw_proxy_queue = 'raw_proxy'
        self.useful_proxy_queue = 'useful_proxy'
        self.log = logger

    def refresh(self):
        """
        fetch proxy into Db by ProxyGetter
        :return:
        """

        for proxy_getter in proxy_getter_methods:
            proxy_set = set()

            # fetch raw proxy
            for proxy in getattr(GetFreeProxy, proxy_getter)():
                if proxy:
                    self.log.info('{func}: fetch proxy {proxy}'.format(func=proxy_getter, proxy=proxy))
                    proxy_set.add(proxy.strip())

            # store raw proxy
            for proxy in proxy_set:
                self.db.changeTable(self.useful_proxy_queue)
                if self.db.exists(proxy): continue
                self.db.changeTable(self.raw_proxy_queue)
                self.db.put(proxy)

    def get(self):
        """
        return a useful proxy
        :return:
        """

        self.db.changeTable(self.useful_proxy_queue)
        proxies = self.db.getAll()
        return random.choice(list(proxies.keys())) if proxies else None

    def delete(self, proxy):
        """
        delete the given proxy from proxy pool
        :param proxy:
        :return:
        """

        self.db.changeTable(self.useful_proxy_queue)
        self.db.delete(proxy)

    def getAll(self):
        """
        get all proxy from proxy pool
        :return: list
        """

        self.db.changeTable(self.useful_proxy_queue)
        proxies = self.db.getAll()
        return list(proxies.keys()) if proxies else list()

    def getNumber(self):
        """
        get number of the raw and useful proxies
        :return: dict
        """

        self.db.changeTable(self.raw_proxy_queue)
        total_raw_proxy = self.db.getNumber()
        self.db.changeTable(self.useful_proxy_queue)
        total_useful_queue = self.db.getNumber()
        return {'raw_proxy': total_raw_proxy, 'useful_proxy': total_useful_queue}

    @staticmethod
    def validUsefulProxy(proxy, logger):
        """
        check whether if the proxy is useful
        if timeout of the proxy over 20s, deprecate it
        :param proxy:
        :return:
        """

        if isinstance(proxy, bytes):
            proxy = proxy.decode('utf8')
        proxies = {"http": "http://{proxy}".format(proxy=proxy)}
        try:
            r = requests.get('http://httpbin.org/ip', proxies=proxies, timeout=20, verify=False)
            if r.status_code == 200:
                logger.info('%s is ok' % proxy)
                return True
        except:
            return False
