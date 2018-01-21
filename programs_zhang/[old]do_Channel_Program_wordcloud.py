# -*- coding:utf-8 -*-
import jieba
from wordcloud import WordCloud,ImageColorGenerator
from scipy.misc import imread  
import matplotlib.pyplot as plt
from multiprocessing import Pool
from collections import Counter
import random
# from jieba import Tokenizer
import jieba
PROCESS_NUM = 8


# channels = []
# programs = []

# f1 = open('ForWordCloud/01_channels.txt','r').readlines()
# f2 = open('ForWordCloud/01_programs.txt','r').readlines()

# s1 = { }
# s2 = { }

# f1 = jieba.cut(f1)
# f1 = f1.split('\n')
# f2 = jieba.cut(f2)

#filenames = ['8_programs10-13','8_programs15-18','8_programs18-21','8_programs21-23','11_programs10-13','11_programs15-18','11_programs18-21','11_programs21-23']
filenames = ['01_programsGold']
def proc(fp):
    w_list = [word for word in fp if len(word) > 1 and word!=u'以播出为准' and word!='NULL']
    c = Counter(w_list)
    return c


def jieba_cut(s):
    return [item for item in jieba.cut(s)]


def main(filename):
    # f1 = open('ForWordCloud/01_channels.txt', 'r').readlines()
    # f1 = [item.decode('utf-8').strip() for item in f1]
    # c1 = proc(f1)
    # pool = Pool(4)
    print 'dealing with:'+filename
    f2 = open('ForWordCloud/'+filename+'.txt', 'r').readlines()
    f2 = [item.decode('utf-8').strip() for item in f2]
    #f2 = pool.map(jieba_cut, f2)
    #f2 = sum(f2, [])
    c2 = proc(f2)
    # back_coloring = imread('background.jpg')
    # channels_wordcloud = WordCloud(background_color='white',
    #                                 margin = 5,
    #                                 width = 1600,
    #                                 height = 900,
    #                                 ).generate_from_frequencies(c1)
    print 'drawing wordcloud:'+filename
    programs_wordcloud = WordCloud(background_color='white',
                                    margin = 5,
                                    width = 1600,
                                    height = 900,
                                    random_state = random.Random()
                                    ).generate_from_frequencies(c2)
    # plt.subplot(1, 2, 1)
    # image_colors = ImageColorGenerator(back_coloring)  
    # plt.figure()

    # plt.imshow(channels_wordcloud)
    # plt.axis('off')
    # plt.subplot(1, 2, 2)
    # plt.imshow(programs_wordcloud)
    # plt.axis('off')
    # plt.show()
    
    # channels_wordcloud.to_file("ChannelWordCloud.png")
    print 'saving image:'+filename
    programs_wordcloud.to_file('ForWordCloud/'+filename+".png")

if __name__ == '__main__':
    for filename in filenames:
        main(filename)


# for w1 in f1:
#     if len(w1)>1:
#         previous_count = s1.get(w1,0)
#         s1[w1] = previous_count+1
# for w2 in f2:
#     if len(w2)>1:
#         previous_count = s2.get(w2,0)
#         s2[w2] = previous_count+1

# channels = sorted(s1.items(),key=lambda (word,count):count, reverse=True)
# print type(channels)
# channels = channels[1:100]
# channels_wordcloud = WordCloud().generate_from_frequencies(s1)

# programs = sorted(s2.items(),key=lambda (word,count):count, reverse=True)
# print programs
# programs = programs[1:100]
# programs_wordcloud = WordCloud().generate_from_frequencies(s2)

# plt.subplot(1,2,1)
# plt.imshow(channels_wordcloud)
# plt.axis('off')
# plt.subplot(1,2,2)
# plt.imshow(programs_wordcloud)
# plt.axis('off')
# plt.show()
