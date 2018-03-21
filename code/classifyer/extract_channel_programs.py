"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author:Gang Zhang
  Date:2018.02.01

  Function:
    1.extracting useful items from original data

    2.extracting channel and prorams from all useful items

    3.normalize channel and programs
"""

import os
import re
import time
import codecs
import psutil
import shutil
import multiprocessing
from program_first_classifyer import Classifyer

DEBUG = True
TMP_PATH = os.getcwd() + '/tmp_result'
ROOT_CATELOGUE = '/media/gzhang/Data'
SCRAPY_PATH = TMP_PATH + '/scrapy_programs'
EXTRACT_ITEM_ERR = TMP_PATH + '/extract_item_error'
EXTRACT_PROGRAM_ERR = TMP_PATH + '/extract_program_error'
EXTRACT_CHANNEL_PROGRAM = TMP_PATH + '/extract_channel_program'


class Preprocess(object):
    """
    Function:
        1.extracting useful items from original data

        2.extracting channel and prorams from all useful items

        3.normalize channel and programs
    """

    def __init__(self):
        self.events = ['21', '5', '96', '97', '6', '7', '13', '14', '17', '23']

    def extract_channel_program(self, err_fw, fr_path, line):
        """
        extract channel and program in one item
        :param err_fw: file writer for err log
        :param fr_path: path of file that current item belongs to
        :param line: current item
        :return: channel and program in current item
        """

        try:
            items = line.strip().split('|')
            if items[1] in ['21', '97', '6', '13', '14', '17', '23']:
                return items[9] + "|" + items[10]
            if items[1] == "5":
                return items[10] + "|" + items[11]
            elif items[1] == "7":
                return items[11] + "|" + items[12]
            elif items[1] == "96":
                return items[-2]
        except IndexError as e:  # invalid data item
            err_fw.write(fr_path + '|' + str(e) + '|' + line + '\n\r')
            return None

    def write_extract_buffer(self, src_folder, channel_program_res):
        """
        flush buffer content into file
        :param src_folder: folder that current buffer content belongs to
        :param channel_program_res: extracted channels and programs in the buffer
        """

        if DEBUG: print(psutil.virtual_memory().percent, src_folder)
        for i in range(len(channel_program_res)):
            path = EXTRACT_CHANNEL_PROGRAM + '/' + src_folder[-2:] + '_' + self.events[i] + '.txt'
            with codecs.open(path, 'a', encoding='utf8') as fw:
                fw.write('\n'.join(set(channel_program_res[i])) + '\n')
            channel_program_res[i] = []

    def extract_events(self, src_folder, des_folder):
        """
        extract events/channels/programs in one single folder
        :param src_folder: source folder
        :param des_folder: destination folder
        """

        extract_item_err_file = EXTRACT_ITEM_ERR + '/' + src_folder[-2:] + '_error.txt'
        extract_program_err_file = EXTRACT_PROGRAM_ERR + '/' + src_folder[-2:] + '_error.txt'
        err_fw = codecs.open(extract_program_err_file, 'w', encoding='utf8', errors='replace')

        src_files = sorted(os.listdir(src_folder))
        channel_program_res = [[] for _ in range(len(self.events))]
        with codecs.open(extract_item_err_file, 'w', encoding='utf8') as err:
            for file_name in src_files:
                if DEBUG: print("enter", src_folder[-2:], file_name)
                fr_path = src_folder + "/" + file_name
                fw_path = des_folder + "/" + file_name[:-3] + "txt"
                fr = codecs.open(fr_path, 'r', encoding='gb18030', errors='replace')
                fw = codecs.open(fw_path, 'w', encoding='utf8', errors='replace')
                for line in fr:
                    try:
                        event_num = line.strip().split('|')[1]
                        if event_num in self.events:
                            res = self.extract_channel_program(err_fw, fr_path, line)
                            if res is None: continue
                            channel_program_res[self.events.index(event_num)].append(res)
                        if event_num in ['21', '5']:
                            fw.write(line)
                    except IndexError as e:  # invalid data item
                        err.write(fr_path + '|' + str(e) + '|' + line + '\n\r')
                fr.close()
                fw.close()

                # when usage of the memory reaches 80 percent, flush the buffer
                if psutil.virtual_memory().percent > 80:
                    self.write_extract_buffer(src_folder, channel_program_res)
        self.write_extract_buffer(src_folder, channel_program_res)

    def extract_all_events(self, process):
        """
        extract events/channels/programs in all folders
        :param process: number of the process
        """

        # src_catelogue = ROOT_CATELOGUE + "/origin_data"
        src_catelogue = "/media/gzhang/Others/original_data"
        des_catelogue = ROOT_CATELOGUE + "/extract_data"

        if not os.path.exists(EXTRACT_ITEM_ERR):
            os.mkdir(EXTRACT_ITEM_ERR)
        if not os.path.exists(EXTRACT_PROGRAM_ERR):
            os.mkdir(EXTRACT_PROGRAM_ERR)
        if os.path.exists(EXTRACT_CHANNEL_PROGRAM):
            shutil.rmtree(EXTRACT_CHANNEL_PROGRAM)
        os.mkdir(EXTRACT_CHANNEL_PROGRAM)

        if DEBUG: print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

        pool = multiprocessing.Pool(process)
        src_folders = sorted(os.listdir(src_catelogue))
        for folder in src_folders:
            src_folder = src_catelogue + "/" + folder
            des_folder = des_catelogue + "/" + folder
            if DEBUG: print("enter", src_folder)
            if not os.path.exists(des_folder):
                os.mkdir(des_folder)
            pool.apply_async(self.extract_events, (src_folder, des_folder))
        pool.close()
        pool.join()

        if DEBUG: print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def cat_sort_uniq_lines(self):
        """
        merge all channel_program files
        :return:
        """

        if DEBUG: print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

        os.chdir(TMP_PATH + '/extract_channel_program')
        for event in self.events:
            if DEBUG: print('start event', event)
            command = 'cat *_' + event + '.txt |sort|uniq > uniq_' + event + '.txt'
            os.system(command)

        if DEBUG: print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def get_channels_programs(self, file_path):
        """
        extract all unique channels and programs in one file
        :param file_path:path of the source file
        :return:ordered list of the channels and programs
        """

        channels, programs = [], []
        with codecs.open(file_path, 'r', encoding='utf8') as fr:
            for line in fr:
                channel_program = line.strip().split('|')
                if len(channel_program) != 2:
                    continue
                if channel_program[0]:
                    channels.append(channel_program[0])
                if channel_program[1]:
                    programs.append(channel_program[1])
        return list(set(channels)), list(set(programs))

    def get_all_channels_programs(self):
        """
        extract all unique channels and programs
        :return all_unique_channels/programs.txt
        """

        channels, programs = [], []
        all_files = sorted(os.listdir(EXTRACT_CHANNEL_PROGRAM))
        for file_name in all_files:
            fr_path = EXTRACT_CHANNEL_PROGRAM + '/' + file_name
            with codecs.open(fr_path, 'r', encoding='utf8') as fr:
                if file_name == 'uniq_96.txt':
                    programs += [line.strip() for line in set(fr.readlines()) if line.strip()]
                else:
                    res = self.get_channels_programs(fr_path)
                    channels += res[0]
                    programs += res[1]

        with codecs.open(TMP_PATH + '/original_unique_channels.txt', 'w', encoding='utf8') as fw:
            fw.write('\n'.join(sorted(set(channels))))
        with codecs.open(TMP_PATH + '/original_unique_programs.txt', 'w', encoding='utf8') as fw:
            fw.write('\n'.join(sorted(set(programs))))

    def normalize_programs(self, src_path, des_path):
        """
        normalize program names
        :param src_path: path of source file
        :param des_path: path of target file
        :return normalized_prorgams.txt
        """

        with codecs.open(src_path, 'r', encoding='utf8') as fr:
            src_programs = [line.strip() for line in set(fr.readlines())]

        handler = Classifyer()
        des_programs = [handler.preprocess_program(program) for program in src_programs]
        des_programs = [program for program in des_programs if program]

        with codecs.open(des_path, 'w', encoding='utf8') as fw:
            fw.write('\n'.join(sorted(set(des_programs))))

    def normalize_channels(self, src_path, des_path):
        """
        normalize channel names
        :param src_path: path of source file
        :param des_path: path of target file
        :return normalized_channels.txt
        """

        with codecs.open(src_path, 'r', encoding='utf8') as fr:
            src_channels = [line.strip() for line in set(fr.readlines())]

        handler = Classifyer()
        des_channels = [handler.preprocess_channel(channel) for channel in src_channels]
        des_channels = [channel for channel in des_channels if channel]

        with codecs.open(des_path, 'w', encoding='utf') as fw:
            fw.write('\n'.join(sorted(set(des_channels))))

    def classify_exist_channels(self, file_path):
        """
        classify all exist channels roughly
        :param file_path:path the target file
        """

        with codecs.open(file_path, 'r', encoding='utf8') as fr:
            regexs = ['^(CCTV|中央)', '卫视$', '影院|电影',
                      '广播|电台|调频|频率|之声$|FM$|AM$',
                      '剧场', '音乐', '纪实|纪录', '音乐', '购',
                      '试播|通道|试验|体验|测试|TEST|test', '\w+']

            channels = [line.strip() for line in fr]
            categories = [[] for _ in range(len(regexs) + 1)]
            for channel in channels:
                for i in range(len(regexs)):
                    if re.search(regexs[i], channel, re.I):
                        categories[i].append(channel)
                        break

        with codecs.open(TMP_PATH + "/nmanual_classified_channels.txt", 'w', encoding='utf8') as fw:
            for i in range(len(categories)):
                fw.write('\n'.join(sorted(set(categories[i]))) + '\n\n')


if __name__ == "__main__":
    handler = Preprocess()

    # handler.extract_all_events(3)
    # handler.cat_sort_uniq_lines()
    # handler.get_all_channels_programs()
    # handler.normalize_programs(TMP_PATH + '/original_unique_programs.txt', TMP_PATH + '/normalized_prorgams.txt')
    # handler.normalize_channels(TMP_PATH + '/original_unique_channels.txt', TMP_PATH + '/normalized_channels.txt')
    # handler.classify_exist_channels(TMP_PATH + '/normalized_channels.txt')
