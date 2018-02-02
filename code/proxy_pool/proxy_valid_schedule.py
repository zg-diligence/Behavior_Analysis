"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author:Gang Zhang
  Date:2018.02.01
"""

from time import sleep
from threading import Thread
from log_handler import LogHandler
from proxy_manager import ProxyManager

FAIL_COUNT = 1  # max number of validation fail times

class ProxyValid(ProxyManager, Thread):
    """
    check proxies in the useful_proxy pool
    """

    def __init__(self):
        log = LogHandler('valid_schedule')
        ProxyManager.__init__(self, log)
        Thread.__init__(self)

    def run(self):
        self.db.changeTable(self.useful_proxy_queue)
        while True:
            proxy_item = self.db.pop()
            while proxy_item:
                proxy = proxy_item.get('proxy')
                counter = proxy_item.get('value', 1)
                if self.validUsefulProxy(proxy, self.log):
                    # success, counter increment by one
                    if counter and int(counter) < 1:
                        self.db.put(proxy, num=int(counter) + 1)
                    else:
                        self.db.put(proxy)
                    self.log.info('ProxyCheck: {} validation pass'.format(proxy))
                else:
                    # fail, counter decreasing by one
                    if counter and int(counter) <= FAIL_COUNT:
                        self.log.info('ProxyCheck: {} fail too many, delete'.format(proxy))
                        self.db.delete(proxy)
                    else:
                        self.db.put(proxy, num=int(counter) - 1)
                    self.log.info('ProxyCheck: {} validation fail'.format(proxy))
                proxy_item = self.db.pop()
            sleep(60 * 5)


class ProxyValidSchedule(object):
    """
    schedule of processes that check proxies in the useful_proxy pool
    """

    def __init__(self):
        pass

    def valid_proxy(self, thread_num=5):
        """
        check whether if the proxies is useful
        :param thread_num: number of threads
        :return:
        """

        thread_list = list()
        for index in range(thread_num):
            thread_list.append(ProxyValid())

        for thread in thread_list:
            thread.daemon = True
            thread.start()

        for thread in thread_list:
            thread.join()


def run():
    ProxyValidSchedule().valid_proxy()
