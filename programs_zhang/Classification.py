# -*- coding:utf-8 -*-
'''
if 事件号 in：
    频道：事件+用户+节目+进出时间+服务器 时间 排序：节目》用户》服务器时间
'''
import PathMaker
import os
from operator import itemgetter
ServerNo = [1,2,3,4,5,7,9,10,11,12]
ServerMin = [06,39,59,06,31,41,49,56,9,48]
ServerSec = [16,53,32,51,59,21,46,12,11,36]
TargetEvents = ['21','5','6','13','17','23','97']
dic = {}
for i_day in range(1,2):
    for i_hour in range(20,21):
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
                        if tmp[1] in TargetEvents:
                            if tmp[1] == '5':#为了处理方便，把事件5的开始时间放到倒数第二位（ServerTime前），这样使得频道名都是tmp[9]
                                StartTime = tmp.pop(6)
                                tmp.insert(-1,StartTime)
                            if tmp[9] == '':
                                continue
                            if tmp[9] in dic:
                                dic[tmp[9]].append(tmp)
                            else:
                                dic[tmp[9]] = [tmp]

                    except:
                        continue
                fp.close()
            except IOError:
                print 'can\'t open file:'+path

        print 'total:', len(dic)


        base_dir = os.path.dirname(__file__)
        outpath = os.path.join(base_dir, "Classification",str(i_day)+"_"+str(i_hour))
        if not os.path.exists(outpath):
            os.makedirs(outpath)

        for key,lists in dic.items():
            #print outpath
            try:
                fp = open(unicode(outpath + '/'+key+'.txt','utf-8'),'w')
                lists.sort(key=itemgetter(10,3,-1))
                for i in lists:
                    fp.write('|'.join(i)+'\n')
                fp.close()
            except IOError:
                print 'error:',key
