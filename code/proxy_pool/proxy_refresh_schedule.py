"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author:Gang Zhang
  Date:2018.02.01
"""

import time
from threading import Thread
from apscheduler.schedulers.blocking import BlockingScheduler
from log_handler import LogHandler
from proxy_manager import ProxyManager

class ProxyRefresh(ProxyManager):
    def __init__(self):
        log = LogHandler('refresh_schedule')
        ProxyManager.__init__(self, log)

    def validProxy(self):
        """
        check proxies in raw_proxy_queue, put useful proxies into useful_proxy_queue
        :return:
        """

        self.log.info('ProxyRefreshSchedule: %s start validProxy' % time.ctime())

        self.db.changeTable(self.raw_proxy_queue)
        raw_proxy_item = self.db.pop()
        remaining_proxies = self.getAll()
        while raw_proxy_item:
            raw_proxy = raw_proxy_item.get('proxy')
            if isinstance(raw_proxy, bytes):
                raw_proxy = raw_proxy.decode('utf8')

            if (raw_proxy not in remaining_proxies) and self.validUsefulProxy(raw_proxy, self.log):
                self.db.changeTable(self.useful_proxy_queue)
                self.db.put(raw_proxy)
                self.log.info('ProxyRefreshSchedule: %s validation pass' % raw_proxy)
            else:
                self.log.info('ProxyRefreshSchedule: %s validation fail' % raw_proxy)

            self.db.changeTable(self.raw_proxy_queue)
            raw_proxy_item = self.db.pop()
            remaining_proxies = self.getAll()

        self.log.info('ProxyRefreshSchedule: %s validProxy complete' % time.ctime())


class ProxyRefreshSchedule(object):
    def __init__(self):
        pass

    def refresh_proxy(self, process_num=30):
        def refreshPool():
            ProxyRefresh().validProxy()

        # get new proxies
        ProxyRefresh().refresh()

        # check new proxies
        threads = []
        for num in range(process_num):
            proc = Thread(target=refreshPool, args=())
            threads.append(proc)

        for num in range(process_num):
            threads[num].daemon = True
            threads[num].start()

        for num in range(process_num):
            threads[num].join()


def run():
    pr = ProxyRefreshSchedule()
    pr.refresh_proxy()

    sched = BlockingScheduler()
    sched.add_job(pr.refresh_proxy, 'interval', minutes=2)  # crawl every 10 minites
    sched.start()
