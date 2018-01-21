# -*- coding:utf-8 -*-
import os
import random
from datetime import datetime, timedelta

def generator():
	while (1):
		t = random.random()
		for item in ChannelClassFreq:
			if item[2]>=t:
				return item

USER_MIN = 800000000000000
USER_MAX = 900000000000000	

AllClasses = ['科教','戏曲','法治','新闻','少儿','音乐','财经','综艺','国际','体育','电影',
			'电视剧','纪录','亲子','茶','汽车','购物','军事','旅游','生活','都市','靓妆',
			'宠物','老年','其他','资讯','娱乐','摄影','美食','收藏','书画','健康','彩票','天气']

ChannelClassFreq = []

base_dir = os.path.dirname(__file__)
inpath = os.path.join(base_dir,'ClassDistribution','Distribution_channel_class_freq_dist=4.txt')
fp = open(inpath,'r')
for line in fp.xreadlines():
	try:

		t_chan, t_clas, t_freq = line.strip().split('\t')
		if t_clas!='#' and float(t_freq)>1e-5:
			ChannelClassFreq.append([t_chan,t_clas,float(t_freq)])
		else:
			for i in AllClasses:
				ChannelClassFreq.append([t_chan,i,float(t_freq)/len(AllClasses)])
	except ValueError:
		continue
fp.close()

ChannelClassFreq.sort(key=lambda x:x[2],reverse=True)

for i in range(1,len(ChannelClassFreq)):
	ChannelClassFreq[i][2] += ChannelClassFreq[i-1][2]

random.seed()
starttime = datetime(2016,5,1,0,0,0)+timedelta(0,random.randint(0,7200))#3hour

outpath = os.path.join(base_dir,'ClassDistribution','generated_data.txt')
fp = open(unicode(outpath,'utf-8'),'w')
for i in range(1000000):
	channel = generator()
	duration = timedelta(0,random.randint(0,7200))
	endtime = starttime + duration
	fp.write('%s\t%s\t%s\t%s\t%s\t%s\n'%(str(random.randint(USER_MIN,USER_MAX)),channel[0],channel[1],str(starttime),str(endtime),str(duration)))
	starttime = endtime