# -*- coding:utf-8 -*-
from wordcloud import WordCloud, ImageColorGenerator
from collections import Counter
import jieba
import matplotlib.pyplot as plt
import time

def decode_data(data):
    data = data.decode('utf8').strip().split('\t')
    return Counter({(data[0],data[1]):int(data[2])})

sum_counter = Counter()
for i in range(1,32):
    THREAD_NUM = 4
    start = time.clock()
    print 'load data...'
    f2 = open('ForProgramClassification/result/'
        +'program_classification_'+str(i)+'_channel_program.txt', 'r').readlines()
    print 'load finished cost ', time.clock() - start


    print 'decode data...'
    time_point = time.clock()
    sub_counter = map(decode_data, f2)
    # print sub_counter
    print 'decode finished cost ', time.clock() - time_point

    for counter in sub_counter:
        sum_counter.update(counter)

fp = open(unicode("ForProgramClassification/result/TOTAL.txt",'utf-8'),'w')
for key,value in sorted(sum_counter.items(),key=lambda x:x[0][0]):
    fp.write((key[0]+'\t'+key[1]+'\t'+str(value)+'\n').encode('utf-8'))
fp.close()