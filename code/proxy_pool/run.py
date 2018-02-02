"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author:Gang Zhang
  Date:2018.02.01
"""

from multiprocessing import Process
from proxy_api import run as ProxyApiRun
from proxy_valid_schedule import run as ValidRun
from proxy_refresh_schedule import run as RefreshRun

def main():
    p_list = list()

    p1 = Process(target=ProxyApiRun, name='ProxyApiRun')
    p_list.append(p1)

    p2 = Process(target=ValidRun, name='ValidRun')
    p_list.append(p2)

    p3 = Process(target=RefreshRun, name='RefreshRun')
    p_list.append(p3)

    for p in p_list:
        p.daemon = True
        p.start()

    for p in p_list:
        p.join()


if __name__ == '__main__':
    main()
