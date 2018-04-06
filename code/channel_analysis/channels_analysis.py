"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author: Gang Zhang
  Creation Date: 2018.4.6
  Last Modified: 2018.4.6

  Function:
    # 特定频道单日收视率变化,单位:时
    # 特定频道单周收视率变化,单位:天
    # 央视频道|卫视频道单日收视率对比
    # 央视频道|卫视频道单周收视率对比
    # 热点节目 单位:日
    # 热点关键词/日 单位:日
"""

import os
import codecs
import multiprocessing

DEBUG = True
ROOT_PATH = '/media/gzhang/Elements/original_data'
EXTRACT_PATH = '/media/gzhang/Data/channel_data'
TMP_PATH = os.path.join(os.getcwd(), 'tmp_result')

categories = ['电影', '电视剧', '新闻', '体育', '财经',
              '法制', '军事', '农业', '纪实', '音乐',
              '戏曲', '少儿', '健康', '时尚', '美食',
              '汽车', '旅游', '综艺', '科教', '生活',  # life is in the later
              '亲子', '购物', '电台', '其它']

class Channel_Analysis(object):
    def __init__(self, channel_dict):
        self.channel_dict = channel_dict            # map from selected chanels to unique channels
        self.channels = list(channel_dict.keys())   # all selected channels
        self.unique_channels = list(channel_dict.values()) # all unique channels

    def write_info_file(self, channel_items, folder_path, hour, mode):
        """
        write event by channel for one hour
        :param channel_items: channel events
        :param folder_path: destionation folder path
        :param hour: hour number
        :param mode: mode for writing file
        :return: none
        """

        for channel in self.unique_channels:
            des_folder_path = os.path.join(EXTRACT_PATH, channel + '/' + folder_path[-2:])
            if not os.path.exists(des_folder_path):
                os.mkdir(des_folder_path)

            file_path = os.path.join(des_folder_path, str(hour) + '.txt')
            with open(file_path, mode) as fw:
                items = channel_items[channel]
                items.sort(key=lambda item: item.split('|')[3])
                fw.write('\n'.join(channel_items[channel]))

    def extract_channel_events(self, folder_path):
        """
        extract channel data for one day
        :param folder_path: source folder for the day
        :return: all users totay
        """

        if DEBUG: print(folder_path)

        # allocate files by hour
        all_files = os.listdir(folder_path)
        all_hour_files = [[] for _ in range(24)]
        for filename in all_files:
            index = int(filename[-10:-8])
            all_hour_files[index].append(filename)

        # extract events by hour
        for i in range(len(all_hour_files)):
            if DEBUG: print(os.getpid(), i)
            hour_files = all_hour_files[i]
            channel_items = dict(zip(self.unique_channels, [[] for _ in range(len(self.unique_channels))]))

            # extract events by channel
            for filename in hour_files:
                file_path = os.path.join(folder_path, filename)
                with codecs.open(file_path, encoding='gb18030', errors='ignore') as fr:
                    for line in fr:
                        try:
                            items = line.strip().split('|')
                            if items[1] != '5': continue

                            ori_channel = items[10]
                            for item in self.channels:
                                if item in ori_channel:
                                    des_channel = self.channel_dict[item]
                                    channel_items[des_channel].append(line.strip())
                        except:
                            pass
            self.write_info_file(channel_items, folder_path, i, 'w')
        if DEBUG: print('over', folder_path)

    def extract_all_channel_events(self, src_catelogue, process_num=4):
        """
        extract all channel events
        :param src_catelogue: source root catelogue
        :param process_num: number of processes
        :return: none
        """

        for channel in self.unique_channels:
            folder_path = os.path.join(EXTRACT_PATH, channel)
            if not os.path.exists(folder_path):
                os.mkdir(folder_path)

        # all_folders = sorted(os.listdir(src_catelogue))
        all_folders = ['01', '09', '17', '18', '19', '30', '31']
        pool = multiprocessing.Pool(process_num)
        for folder in all_folders:
            folder_path = os.path.join(src_catelogue, folder)
            pool.apply_async(self.extract_channel_events, (folder_path, ))
        pool.close()
        pool.join()

    def compute_channel_ratings(self, channel_path):
        pass

    def compute_all_channel_ratings(self):
        pass


if __name__ == '__main__':
    if not os.path.exists(EXTRACT_PATH):
        os.mkdir(EXTRACT_PATH)

    with open('channels.txt') as fr:
        items = fr.read().strip().split('\n')
        channel_dict = dict([item.split(' ') for item in items])

    handler = Channel_Analysis(channel_dict)
    handler.extract_all_channel_events(ROOT_PATH, 4)
