# -*- coding:utf-8 -*-

import PathMaker
import os
from datetime import datetime,timedelta
from operator import itemgetter
        

ServerNo = [1,2,3,4,5,7,9,10,11,12,1,1,2,2,3,3,4,4,5,5,7,7,9,9,9,10,10,10,11,11,11,12,12]
ServerMin = [06,39,59,06,31,41,49,56,9,48,16,46,21,51,36,16,46,31,57,47,56,45,01,06,45,06,16,45,11,36,45,31,11]
ServerSec = [16,53,32,51,59,21,46,12,11,36,54,55,55,55,54,54,54,55,29,29,53,44,56,56,47,54,54,45,51,52,42,55,55]
TargetEvents = ['21','5','6','13','17','23','97','7']
MainTargetEvents = ['21','5','6','13','17','23','97','7']


class ItemStruct():
    def __init__(self, line):
        item = line.split('|')
        self.messageID = int(item[0])
        self.eventID = item[1]
        self.randomSeq = item[2]
        self.CACardNo = item[3]
        self.Freq = item[4]
        self.ServerTime = item[-1]
        self.endTime = datetime.strptime(item[5], "%Y.%m.%d %H:%M:%S") if self.eventID!='20' else None # 除了20心跳事件没有时间外其他时间的事件位置固定
        if self.eventID in MainTargetEvents:
            if self.eventID == '5':#为了处理方便，把事件5的开始时间放到倒数第二位（ServerTime前），这样使得频道名都是tmp[9]
                StartTime = item.pop(6)
                item.insert(-1,StartTime)
                self.startTime = datetime.strptime(StartTime, "%Y.%m.%d %H:%M:%S")
            if self.eventID == '7':
                StartVol = item.pop(7)
                StartTime = item.pop(6)
                item.insert(-1,StartTime)
                item.insert(-1,StartVol)
                self.startTime = StartTime
            if self.eventID == '96':#【10->11】响应时间 【11->12】当前时间 【12->10】节目名
                programName = item.pop(12)
                item.insert(10,programName)


            self.channelName = item[9] if self.eventID!='96' else 'None'
            self.programName = item[10]



ChannelClass = { }
ChannelFreq  = { }
base_dir = os.path.dirname(__file__)
inpath = os.path.join(base_dir,'ClassDistribution','channel_class.txt')
fp = open(inpath,'r')
for line in fp.xreadlines():
    t_chan, t_clas = line.decode('gbk').encode('utf8').split('\t')
    t_clas = t_clas.strip()
    ChannelClass[t_chan] = t_clas if t_clas!='' else '#'
    ChannelFreq[t_chan] = 0
fp.close()

total = 0
for i_day in [1,5,9,13,17,21,25,29]:#range(1,4):
    for i_hour in range(0,24):
        # = {}
        # for ChannelName in ChannelNames.keys():
        #     freq[ChannelName] = set()
        for i in range(len(ServerNo)):
            date = PathMaker.NTADate(i_day,5,2016)
            time = PathMaker.NTATime(i_hour,ServerMin[i],ServerSec[i])
            path = PathMaker.make_in_path(ServerNo[i],date,time)
            print path
            try:
                fp = open(path, 'r')
                fp.readline()

                for line in fp:
                    try:
                        tmp = ItemStruct(line.decode('gbk').encode('utf8').strip())
                        if tmp.eventID in MainTargetEvents:
                            if tmp.channelName in ChannelFreq:
                                ChannelFreq[tmp.channelName] += 1
                            else:
                                ChannelFreq[tmp.channelName] = 1
                                ChannelClass[tmp.channelName] = '#'
                            total += 1
                    except:
                        continue
                fp.close()
            except IOError:
                print 'can\'t open file:'+path

        #print 'total:', len(dic)


outpath = os.path.join(base_dir,'ClassDistribution','Distribution_channel_class_freq_dist=4.txt')
fp = open(unicode(outpath,'utf-8'), 'w')
for channelName, freq in ChannelFreq.items():
    try:
        fp.write(channelName+'\t'+ChannelClass[channelName]+'\t'+str(freq*1.0/total)+'\n')
    except IOError:
        print 'error:',i_hour

fp.close()
