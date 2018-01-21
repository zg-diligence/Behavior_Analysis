# -*- coding:utf-8 -*-
'''
if 事件号 in：
    频道：事件+用户+节目+进出时间+服务器 时间 排序：节目》用户》服务器时间
'''
import PathMaker
import os
from operator import itemgetter
ServerNo = [1,2,3,4,5,7,9,10,11,12,1,1,2,2,3,3,4,4,5,5,7,7,9,9,9,10,10,10,11,11,11,12,12]
ServerMin = [06,39,59,06,31,41,49,56,9,48,16,46,21,51,36,16,46,31,57,47,56,45,01,06,45,06,16,45,11,36,45,31,11]
ServerSec = [16,53,32,51,59,21,46,12,11,36,54,55,55,55,54,54,54,55,29,29,53,44,56,56,47,54,54,45,51,52,42,55,55]
TargetEvents = ['21','5','6','13','17','23','97','7']
ChannelNames = {'CCTV-1':'CCTV-1高清','CCTV-4':'CCTV-4高清','江苏卫视':'江苏卫视高清','湖南卫视':'湖南卫视高清'}

#for ChannelName, ChannelName2 in ChannelNames.items():
for i_day in range(9,16):#range(1,4):
    for i_hour in range(0,24):
        freq = {'CCTV-1':set(),'CCTV-4':set(),'江苏卫视':set(),'湖南卫视':set()}
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
                            if tmp[1] == '7':
                                StartVol = tmp.pop(7)
                                StartTime = tmp.pop(6)
                                tmp.insert(-1,StartTime)
                                tmp.insert(-1,StartVol)
                            for ChannelName, ChannelName2 in ChannelNames.items():
                                if tmp[9] == ChannelName or tmp[9] == ChannelName2:
                                    if tmp[3] not in freq[ChannelName]:
                                        freq[ChannelName].add(tmp[3])
                    except:
                        continue
                fp.close()
            except IOError:
                print 'can\'t open file:'+path

        #print 'total:', len(dic)

        base_dir = os.path.dirname(__file__)
        outpath = os.path.join(base_dir, "Stat2C2W2weeks--9-16try")
        if not os.path.exists(outpath):
            os.makedirs(outpath)
        for ChannelName in ChannelNames.keys():
            try:
                fp = open(unicode(outpath + '/'+ChannelName+'.txt','utf-8'), 'a')
                fp.write(str(i_hour)+' '+str(len(freq[ChannelName]))+'\n')
                fp.close()
            except IOError:
                print 'error:',i_hour

