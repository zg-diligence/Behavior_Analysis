"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author: Gang Zhang
  Creation Date: 2018.4.11
  Last Modified: 2018.4.12

  Function:
    Visualize all rating statistics
"""

import os
import itertools
import numpy as np
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['FZKai-Z03']

DEBUG = True
EXTRACT_PATH = '/media/gzhang/Data/channel_data'
TMP_PATH = os.path.join(os.getcwd(), 'tmp_result')
PIC_PATH_1 = TMP_PATH + '/rating_analyze_pic_1'
PIC_PATH_2 = TMP_PATH + '/rating_analyze_pic_2'


class Channel_Analyze(object):
    def __init__(self):
        pass

    def read_hour_ratings(self, folder_path, channels):
        """
        read hour ratings from file
        :param folder_path: folder path count ratings
        :param channels: name of all channels
        :return: ratings of 24 * 31 hours for all channels
        """

        channel_ratings = {}
        for channel in channels:
            file_path = os.path.join(folder_path, channel + '.txt')
            if not os.path.exists(file_path):
                continue
            with open(file_path) as fr:
                ratings = [line.strip().split('|') for line in fr]
                ratings = np.array([line for line in ratings if ratings.index(line) not in [2, 22, 28]],
                                   dtype=np.float32)
                channel_ratings[channel] = ratings
        return channel_ratings

    def display_pic_1(self, title, data):
        plt.figure(figsize=(16, 8))
        X_ticks = [str(i) for i in range(24)]
        X = list(range(24))
        plt.figure()
        plt.title(title)
        for channel, ratings in data:
            plt.plot(X, ratings, label=channel)
        plt.xlabel("hour")
        plt.ylabel("rating")
        plt.xticks(X, X_ticks)
        plt.legend()
        plt.grid(b=True)
        plt.savefig(PIC_PATH_1 + '/' + title + '.jpg', format='jpg', dpi=400)

    def display_avg_ratings(self, channel_ratings):
        """
        average ratings of total 28 days
        :param channel_ratings: ratings of 24 * 31 hours for all channels
        :return: none
        """

        channel_avg_ratings = {}
        for channel, ratings in channel_ratings.items():
            sum_ratings = np.zeros(24)
            for rating in ratings:
                sum_ratings += rating
            avg_ratings = sum_ratings / 28
            avg_ratings = np.array([np.round(rat, 4) for rat in avg_ratings])
            channel_avg_ratings[channel] = avg_ratings
        sorted_ratings = sorted(channel_avg_ratings.items(), key=lambda item: max(item[1]), reverse=True)

        self.display_pic_1('_央视top5 月平均收视率变化曲线图', sorted_ratings[:5])
        self.display_pic_1('_总体top10 月平均收视率变化曲线图', sorted_ratings[:10])
        tmp_ratings = [item for item in sorted_ratings if 'CCTV' not in item[0]]
        self.display_pic_1('_卫视top5 月平均收视率变化曲线图', tmp_ratings[:5])

        for channel, ratings in sorted_ratings:
            self.display_pic_1(channel, [(channel, ratings), ])

        # maximum rating ranking
        max_ratings = []
        for channel, ratings in sorted_ratings:
            max_ratings.append((channel, max(ratings)))

        plt.figure(figsize=(8, 4))
        y = [rat for _, rat in max_ratings][:10]
        x = [ch for ch, _ in max_ratings][:10]
        plt.bar(np.arange(len(x)), y, width=0.5)
        plt.xticks(np.arange(len(x)), x)
        for a, b in enumerate(y):
            plt.text(a, b + 0.0001, '%.4f' % b, ha='center', va='bottom', fontsize=11)
        plt.title('top10 单日最高收视率对比图')
        plt.savefig(PIC_PATH_1 + '/total_ranking.jpg', format='jpg', dpi=400)
        plt.close()

    def display_pic_2(self, title, data, channels):
        plt.figure(figsize=(12, 6))
        X_ticks = [str(i) for i in range(0, 24, 6)]
        new_ticks = []
        for it in X_ticks * 14:
            new_ticks.append(it)
            new_ticks += [''] * 5
        new_ticks.pop()

        X = list(range(0, 336))
        plt.title(title)
        for channel, ratings in data:
            if channel in channels:
                plt.plot(X, ratings, label=channel)

        plt.xlabel("hour")
        plt.ylabel("rating")
        plt.xticks(X, new_ticks)
        plt.legend()
        plt.savefig(PIC_PATH_2 + '/' + title + '.jpg', format='jpg', dpi=600)

    def display_all_ratings(self, channel_ratings):
        """
        ratings from 5.6 to 5.19 (two weeks) for each channel
        :param channel_ratings: ratings of 24 * 31 hours for all channels
        :return: none
        """

        new_channel_ratings = {}
        for channel, ratings in channel_ratings.items():
            tmp = list(itertools.chain(*ratings[6:20]))
            new_channel_ratings[channel] = tmp[::]
        sorted_ratings = sorted(new_channel_ratings.items(), key=lambda item: item[0])

        channels = ['东方卫视', '湖南卫视', '浙江卫视', '江苏卫视', '北京卫视']
        self.display_pic_2('__卫视top5 两周收视率变化曲线', sorted_ratings, channels)

        channels = ['CCTV-1', 'CCTV-4', 'CCTV-3', 'CCTV-5', 'CCTV-6']
        self.display_pic_2('__央视top5 两周收视率变化曲线', sorted_ratings, channels)

        channels = ['CCTV-1', 'CCTV-4', '湖南卫视', '浙江卫视', '江苏卫视']
        self.display_pic_2('__卫央top5 两周收视率变化曲线', sorted_ratings, channels)

        for channel in channel_ratings.keys():
            self.display_pic_2(channel, sorted_ratings, [channel, ])


if __name__ == '__main__':
    if not os.path.exists(PIC_PATH_1):
        os.mkdir(PIC_PATH_1)
    if not os.path.exists(PIC_PATH_2):
        os.mkdir(PIC_PATH_2)

    with open('channels.txt') as fr:
        items = fr.read().strip().split('\n')
        channel_set = set([item.split(' ')[1] for item in items])

    handler = Channel_Analyze()
    ratings_folder = os.path.join(TMP_PATH, 'ratings_by_hour')
    res = handler.read_hour_ratings(ratings_folder, channel_set)
    # handler.display_avg_ratings(channel_ratings=res)
    # handler.display_all_ratings(channel_ratings=res)
