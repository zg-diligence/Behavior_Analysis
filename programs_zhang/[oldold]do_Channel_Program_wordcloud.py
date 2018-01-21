# -*- coding:utf-8 -*-


import jieba
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from multiprocessing import Pool

PROCESS_NUM = 8

channels = []
programs = []

f1 = open('ForWordCloud/01_channels.txt','r').readlines()
f2 = open('ForWordCloud/01_programs.txt','r').read()

s1 = { }
s2 = { }

f1 = map(strip,f1)
#f1 = f1.split('\n')
f2 = jieba.cut(f2)

for w1 in f1:
    if len(w1)>1:
        previous_count = s1.get(w1,0)
        s1[w1] = previous_count+1
for w2 in f2:
    if len(w2)>1:
        previous_count = s2.get(w2,0)
        s2[w2] = previous_count+1

#channels = sorted(s1.items(),key=lambda (word,count):count, reverse=True)
#print type(channels)
#channels = channels[1:100]
channels_wordcloud = WordCloud().generate_from_frequencies(s1)

#programs = sorted(s2.items(),key=lambda (word,count):count, reverse=True)
#print programs
#programs = programs[1:100]
programs_wordcloud = WordCloud().generate_from_frequencies(s2)

plt.subplot(1,2,1)
plt.imshow(channels_wordcloud)
plt.axis('off')
plt.subplot(1,2,2)
plt.imshow(programs_wordcloud)
plt.axis('off')
plt.show()
