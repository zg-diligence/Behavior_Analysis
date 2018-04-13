"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author: Gang Zhang
  Creation Date: 2018.4.6
  Last Modified: 2018.4.13

  Function:
    15个央视频道 and 31个卫视频道
    Count ratings by hour (24 * 28) for all channels
    statistics result is saved by 28 lines for 28 days, 24 items one line for 24 hours seperated by |
"""

import os
import time
import codecs
import multiprocessing
from datetime import datetime, timedelta

DEBUG = True
ROOT_PATH = '/media/gzhang/Elements/original_data'
SOURCE_DATA = '/media/gzhang/Elements/extract_data'
EXTRACT_PATH = '/media/gzhang/Data/channel_data'
TMP_PATH = os.path.join(os.getcwd(), 'tmp_result')


class CountRatings(object):
    def __init__(self, channel_dict):
        self.channel_dict = channel_dict  # map from selected chanels to unique channels
        self.channels = list(channel_dict.keys())  # all selected channels
        self.unique_channels = list(channel_dict.values())  # all unique channels

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

        all_folders = sorted(os.listdir(src_catelogue))
        pool = multiprocessing.Pool(process_num)
        for folder in all_folders:
            folder_path = os.path.join(src_catelogue, folder)
            pool.apply_async(self.extract_channel_events, (folder_path,))
        pool.close()
        pool.join()

    def count_number(self, folder_path):
        """
        count all user ids for one day
        :param folder_path: folder path of source data for the day
        :return: dict of user ids, value is 1
        """

        if DEBUG: print(folder_path)
        all_files = os.listdir(folder_path)
        users = {}
        for filename in all_files:
            file_path = os.path.join(folder_path, filename)
            with open(file_path) as fr:
                for line in fr:
                    try:
                        user_id = line.split('|')[3]
                    except:
                        continue
                    else:
                        users[user_id] = 1
        return users

    def count_total_number(self, catelogue):
        """
        count all user ids
        :param catelogue: root catelogue of source data
        :return: none
        """

        processes = []
        pool = multiprocessing.Pool(4)
        folders = os.listdir(catelogue)
        for folder in folders:
            folder_path = os.path.join(catelogue, folder)
            processes.append(pool.apply_async(self.count_number, (folder_path,)))
        pool.close()
        pool.join()

        all_users = []
        for p in processes:
            all_users += list(p.get().keys())
        all_users = set(all_users)
        if DEBUG: print(len(all_users))
        with open(TMP_PATH + '/user_ids.txt', 'w') as fw:
            fw.write('\n'.join(all_users))

    def update_ratings(self, TV_tts, start_tt, end_tt):
        """
        add a record for one TV
        :param TV_tts: ratings array for one TV
        :param start_tt: starting time of the new record
        :param end_tt: ending time of the new record
        :return: none
        """

        init = datetime(year=2016, month=5, day=1, hour=0, minute=0, second=0)

        # check unqualified record
        if end_tt <= init or start_tt >= init + timedelta(days=31):
            return

        # check overflowed revord
        if end_tt >= init + timedelta(days=31):
            end_tt = init + timedelta(days=30, hours=23, minutes=59, seconds=59)
        if start_tt <= init:
            start_tt = init

        # adding the time
        start_index = int((start_tt - init).total_seconds() // 3600)
        end_index = int((end_tt - init).total_seconds() // 3600)
        lhs_tt, rhs_tt = start_tt, end_tt
        for index in range(start_index, end_index):
            tmp_tt = init + timedelta(hours=index + 1)
            TV_tts[index] += (tmp_tt - lhs_tt).total_seconds()
            lhs_tt = tmp_tt
        tmp_tt = init + timedelta(hours=end_index)
        TV_tts[end_index] += (rhs_tt - tmp_tt).total_seconds()

    def compute_channel_ratings(self, channel_path, threshold):
        """
        compute rating of one TV by hour
        :param channel_path: folder path of the TV
        :param threshold: time limit of a watcing record
        :return:none
        """

        if DEBUG: print(channel_path)
        day_folders = sorted(os.listdir(channel_path))
        TV_tts = [0 for _ in range(31 * 24)]
        for i in range(len(day_folders)):
            folder = day_folders[i]
            folder_path = os.path.join(channel_path, folder)
            hour_files = sorted(os.listdir(folder_path), key=lambda item: int(item[:-4]))
            for filename in hour_files:
                file_path = os.path.join(folder_path, filename)
                with open(file_path) as fr:
                    for line in fr:
                        try:
                            items = line.strip().split('|')
                            start_tt = datetime.strptime(items[6], '%Y.%m.%d %H:%M:%S')
                            end_tt = datetime.strptime(items[5], '%Y.%m.%d %H:%M:%S')
                            init = datetime(year=2016, month=5, day=1, hour=0, minute=0, second=0)
                            if (end_tt - init).days - i > 3: continue  # check noise event
                        except:
                            continue
                        else:
                            last_tt = end_tt - start_tt
                            if last_tt.seconds < threshold: continue  # check whether up to the time limit of access
                            self.update_ratings(TV_tts, start_tt, end_tt)
        if DEBUG: print("out:" + channel_path)

        # write ratings of the TV info file, a single line for one day
        ratings = ['%.6f' % (item / (657229 * 3600)) for item in TV_tts]
        folder_path = os.path.join(TMP_PATH, 'ratings_by_hour')
        file_path = os.path.join(folder_path, os.path.basename(channel_path) + '.txt')
        with open(file_path, 'w') as fw:
            for i in range(31):
                fw.write('|'.join(ratings[i * 24:i * 24 + 24]) + '\n')

    def compute_all_channel_ratings(self, catelogue, threshold=15, process_num=4):
        """
        compute ratings for all TVs
        :param catelogue: root catelogue for all TV data
        :param threshold: time limit of one watching record
        :param process_num: num of the processes
        :return:none
        """

        TV_folders = sorted(os.listdir(catelogue))
        pool = multiprocessing.Pool(process_num)
        for folder in TV_folders:
            folder_path = os.path.join(catelogue, folder)
            pool.apply_async(self.compute_channel_ratings, (folder_path, threshold))
        pool.close()
        pool.join()


if __name__ == '__main__':
    if not os.path.exists(EXTRACT_PATH):
        os.mkdir(EXTRACT_PATH)

    with open(TMP_PATH + '/channels.txt') as fr:
        items = fr.read().strip().split('\n')
        channel_dict = dict([item.split(' ') for item in items])

    if DEBUG: print(time.strftime("%Y-%m-%d %X", time.localtime()))

    handler = CountRatings(channel_dict)
    # handler.extract_all_channel_events(ROOT_PATH, 4)
    # handler.count_total_number(SOURCE_DATA)
    handler.compute_all_channel_ratings(EXTRACT_PATH, threshold=15)

    if DEBUG: print(time.strftime("%Y-%m-%d %X", time.localtime()))
