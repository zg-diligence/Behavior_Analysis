# -*- coding:utf-8 -*-
from wordcloud import WordCloud, ImageColorGenerator
from multiprocessing import Pool
from collections import Counter
import jieba
import matplotlib.pyplot as plt
import time
import random

def jieba_cut(s):
    if len(s)>1:
        return [(item,int(s[1])) for item in jieba.cut(s[0]) if s[0]!=u'以播出为准' and s[0]!='NULL']
    else:
        return []


def proc(fp):
    w_list = sum(fp, [])
    w_list = [word for word in w_list if len(word[0]) > 1]
    w_list = dict(w_list)
    c = Counter(w_list)
    return c


def decode_data(data):
    return data.decode('utf8').strip().split('\t')[-2:]

def main(filename):
    THREAD_NUM = 4
    start = time.clock()
    print 'load data...'
    f2 = open('ForProgramClassification/'+filename, 'r').readlines()
    print 'load finished cost ', time.clock() - start

    print 'decode data...'
    time_point = time.clock()
    pool = Pool(THREAD_NUM)
    string_list = pool.map(decode_data, f2)
    pool.close()
    print 'decode finished cost ', time.clock() - time_point

    print 'cut data...'
    time_point = time.clock()
    pool = Pool(THREAD_NUM)
    list_list = pool.map(jieba_cut, string_list)
    pool.close()
    print 'cut finished cost ', time.clock() - time_point

    print 'map count...'
    time_point = time.clock()
    pool = Pool(THREAD_NUM)
    sub_list_list = [list_list[i::THREAD_NUM] for i in xrange(THREAD_NUM)]
    sub_counter = pool.map(proc, sub_list_list)
    pool.close()
    print 'map count finished cost ', time.clock() - time_point

    print 'merge counter...'
    time_point = time.clock()
    sum_counter = Counter()
    for counter in sub_counter:
        sum_counter.update(counter)
    print 'merge counter finished cost ', time.clock() - time_point

    print 'draw graph...'
    time_point = time.clock()
    programs_wordcloud = WordCloud(background_color='white',
                                    margin = 5,
                                    width = 1600,
                                    height = 900,
                                    random_state = random.Random()
                                    ).generate_from_frequencies(sum_counter)
    plt.imshow(programs_wordcloud)
    plt.axis('off')
    print 'draw graph finished cost ', time.clock() - time_point
    print 'deal finished all cost ', time.clock() - start

    #plt.show()

    programs_wordcloud.to_file("ForWordCloud/WordCloud_"+filename+".png")


if __name__ == '__main__':
    for i in range(30,32):
        main(str(i)+'_channel_program.txt')
