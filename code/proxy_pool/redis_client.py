"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author:Gang Zhang
  Date:2018.02.01
"""

import random
from redis import Redis
from redis.connection import BlockingConnectionPool

class RedisClient(object):
    """
    Redis client
    raw_proxy：raw proxies, key => proxy ip:port，value => None
    useful_proxy：useful proxies, key => proxies ip:port，value is the counter, inital value is 1
    """

    def __init__(self, name, host, port):
        self.name = name
        self.session = Redis(connection_pool=BlockingConnectionPool(host=host, port=port))

    def get(self, proxy):
        """
        retur infomation of the given proxy
        :param proxy:
        :return:
        """

        data = self.session.hget(name=self.name, key=proxy)
        return data.decode('utf-8') if data else None

    def put(self, proxy, num=1):
        """
        put an new proxy into the proxy pool
        :param proxy:
        :param num:
        :return:
        """

        data = self.session.hincrby(self.name, proxy, num)
        return data

    def delete(self, proxy):
        """
        delete the given proxy from proxy pool
        :param proxy:
        :return:
        """

        self.session.hdel(self.name, proxy)

    def update(self, proxy, value):
        self.session.hincrby(self.name, proxy, value)

    def pop(self):
        """
        pop a proxy from proxy pool randomly
        :return:
        """

        proxies = self.session.hkeys(self.name)
        if proxies:
            proxy = random.choice(proxies)
            value = self.session.hget(self.name, proxy)
            self.delete(proxy)
            return {'proxy': proxy.decode('utf-8'),
                    'value': value.decode('utf-8') if value else value}
        return None

    def exists(self, proxy):
        """
        check whether if the given proxy is in the proxy pool
        :param proxy:
        :return:
        """

        return self.session.hexists(self.name, proxy)

    def getAll(self):
        """
        get all the proxies in the proxy pool
        :return: dict{proxy: value}
        """

        item_dict = self.session.hgetall(self.name)
        return {proxy.decode('utf8'): value.decode('utf8') for proxy, value in item_dict.items()}

    def getNumber(self):
        """
        get number of the proxies in the proxy pool
        :return:
        """

        return self.session.hlen(self.name)

    def changeTable(self, name):
        """
        change current proxy pool
        :param name:
        :return:
        """

        self.name = name
