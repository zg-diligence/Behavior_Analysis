# -*- coding:utf-8 -*-

import PathMaker
import os
from operator import itemgetter
#import TrieTree

#trie = TrieTree.Trie()
ServerNo = [1,2,3,4,5,7,9,10,11,12,1,1,2,2,3,3,4,4,5,5,7,7,9,9,9,10,10,10,11,11,11,12,12]
ServerMin = [06,39,59,06,31,41,49,56,9,48,16,46,21,51,36,16,46,31,57,47,56,45,01,06,45,06,16,45,11,36,45,31,11]
ServerSec = [16,53,32,51,59,21,46,12,11,36,54,55,55,55,54,54,54,55,29,29,53,44,56,56,47,54,54,45,51,52,42,55,55]
dic = { }
for i_day in [6]:#range(1,2):
    for i_hour in range(0,24):
        for i in range(len(ServerNo)):
            date = PathMaker.NTADate(i_day,5,2016)
            time = PathMaker.NTATime(i_hour,ServerMin[i],ServerSec[i])
            path = PathMaker.make_in_path(ServerNo[i],date,time)
            print path
            try:
                fp = open(path, 'r')
                fp.readline()
                t = 0
                for line in fp:
                    t += 1
                    try:
                        tmp = line.decode('gbk').encode('utf8').strip().split('|')
                    #except UnicodeDecodeError:
                    #    continue
                    #try:
                        key = tmp[3]
                        if key not in dic:
                            dic[key] = 1
                        else:
                            dic[key] += 1
                    except:
                        continue
                fp.close()
            except IOError:
                print 'can\'t open file:'+path

print 'total:',len(dic)

base_dir = os.path.dirname(__file__)
outpath = os.path.join(base_dir, "SelectUser")
if not os.path.exists(outpath):
    os.makedirs(outpath)
fp = open(outpath + '/'+'users.txt','w')
for key,num in dic.items():
    #print outpath
    fp.write(str(key)+' '+str(num)+'\n')

fp.close()