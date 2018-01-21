# -*- coding:utf-8 -*-
from wordcloud import WordCloud, ImageColorGenerator
from multiprocessing import Pool
from collections import Counter
import jieba
import matplotlib.pyplot as plt
import time

'''
统计结果不正确，貌似中间过程那里没有初始化
'''



def jieba_cut(s):
    # if len(s)>2:
    #     return [((s[0],item),int(s[2])) for item in jieba.cut(s[1])]
    # else:
    #     return []

    if len(s)>2:
        if len(s)>3:
            s = [s[0],''.join(s[1:-1]),s[-1]]
        return [((s[0],item),int(s[2])) for item in jieba.cut(s[1])]
        # ret = []
        # for item in jieba.cut(s[1]):
        #     ret.append(((s[0],item),int(s[2])))
        # return ret
    else:
        return []



def proc(fp):
    w_list = sum(fp, [])
    w_list = [word for word in w_list if len(word[0][1]) >= 2 and word[0][1]!='NULL' and word[0][1]!=u'以播出为准']
    # w_list = [word for word in w_list]

    w_list = dict(w_list)
    c = Counter(w_list)
    return c


def decode_data(data):
    return data.decode('utf8').strip().split('\t')

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

    # print 'draw graph...'
    # time_point = time.clock()
    # programs_wordcloud = WordCloud(background_color='white',
    #                                 margin = 5,
    #                                 width = 1600,
    #                                 height = 900,
    #                                 random_state = random.Random()
    #                                 ).generate_from_frequencies(sum_counter)
    # plt.imshow(programs_wordcloud)
    # plt.axis('off')
    # print 'draw graph finished cost ', time.clock() - time_point
    print 'deal finished all cost ', time.clock() - start

    #plt.show()
    # programs_wordcloud.to_file("ForWordCloud/WordCloud_"+filename+".png")

    # print sum_counter

    fp = open(unicode("ForProgramClassification/result/program_classification_"+filename,'utf-8'),'w')
    for key,value in sorted(sum_counter.items(),key=lambda x:x[0][0]):
        fp.write((key[0]+'\t'+key[1]+'\t'+str(value)+'\n').encode('utf-8'))
    fp.close()

if __name__ == '__main__':
    for i in range(1,32):#range(16,32):
        # try:
        #     main(str(i)+'_channel_program.txt')
        # except UnicodeEncodeError:
        #     continue
        print '\n\n\n\n\n'
        print '---------------PROCESSING DAY '+str(i)+'---------------'
        main(str(i)+'_channel_program.txt')

