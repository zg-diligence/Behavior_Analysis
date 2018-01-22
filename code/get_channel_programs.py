import os
import re
import time
import codecs
import psutil
import shutil
import multiprocessing
from string import punctuation as env_punc
from zhon.hanzi import punctuation as chs_punc

DEBUG = True
TMP_PATH = os.getcwd() + '/tmp_result'
ROOT_CATELOGUE = '/media/gzhang/Data'
EXTRACT_ITEM_ERR = TMP_PATH + '/extract_item_error'
EXTRACT_PROGRAM_ERR = TMP_PATH + '/extract_program_error'
EXTRACT_CHANNEL_PROGRAM = TMP_PATH + '/extract_channel_program'

class Preprocess(object):
    def __init__(self):
        self.events = ['21','5','96','97','6','7','13','14','17', '23']

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
        except IndexError as e: # invalid data item
            err_fw.write(fr_path + '|'  + str(e) + '|' + line +'\n\r')
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
                            channel_program_res[self.events.index(event_num)].append(res)
                        if event_num in ['21', '5']:
                            fw.write(line)
                    except IndexError as e: # invalid data item
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

        src_catelogue = ROOT_CATELOGUE + "/origin_data"
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
        src_folders = os.listdir(src_catelogue)
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
                if file_name[-6:-4] == '96':
                    programs += [line.strip() for line in set(fr.readlines()) if line.strip()]
                else:
                    res = self.get_channels_programs(fr_path)
                    channels += res[0]
                    programs += res[1]
        with codecs.open(TMP_PATH + '/all_unique_channels.txt', 'w', encoding='utf8') as fw:
            fw.write('\n'.join(sorted(set(channels))))
        with codecs.open(TMP_PATH + '/all_unique_programs.txt', 'w', encoding='utf8') as fw:
            fw.write('\n'.join(sorted(set(programs))))

    def normalize_programs(self, file_path):
        """
        normalize program names
        :param file_path: path of target file
        :return normalized_prorgams.txt
        """

        programs, regexes = [], []
        chs_num = '一二三四五六七八九十'
        punctuations = env_punc + chs_punc
        unvisible_chars = ''.join([chr(i) for i in range(32)])
        regexes.append(re.compile('.*(报复|反复|回复|修复)$'))
        regexes.append(re.compile('(限免|中文版|英文版|回看|复播|重播|复|[上中下尾]|[ⅡⅢI]+)$'))
        regexes.append(re.compile('\s'))                            # remove space chars
        regexes.append(re.compile('[%s]' % punctuations))           # remove punctuations
        regexes.append(re.compile('[%s]' % unvisible_chars))        # remove control chars
        regexes.append(re.compile('^(HD|3D)|(HD|SD|3D|TV|杜比)$'))   # remove program marks
        regexes.append(re.compile('(\d{2,4}年)*\d{1,2}月\d{1,2}日'))       # remove date
        regexes.append(re.compile('(第([%s]+|\d+)[部季集]+)$' % chs_num))  # remove serial number
        regexes.append(re.compile('(\d+|[%s]+)$' % chs_num))        # remove serial number

        with codecs.open(file_path, 'r', encoding='utf8') as fr:
            for line in fr:
                tmp = line.strip()
                for regex in regexes[2:]:
                    tmp = re.sub(regex, '', tmp)
                if not re.match(regexes[0], tmp):
                    tmp = re.sub(regexes[1], '', tmp)

                # remove serial number at the middle of the program name
                res = re.search('第([%s]+|\d+)[部集季]+' % chs_num, tmp)
                if res and not re.match('^\d+', tmp):
                    tmp = tmp[:res.span()[0]]

                # remove serial number at the end of the program name again
                tmp = re.sub('(\d+|[%s]+)$' % chs_num, '', tmp)
                tmp = re.sub('(第([%s]+|\d+)[部季集]+)$' % chs_num, '', tmp)

                # remove chinese garbled
                if re.search('[^(\w+\-)]', tmp):continue
                if tmp: programs.append(tmp)

        with codecs.open(TMP_PATH + '/normalized_prorgams.txt', 'w', encoding='utf8') as fw:
            fw.write('\n'.join(sorted(set(programs))))

    def normalize_channels(self, file_path):
        """
        normalize channel names
        :param file_path: path of the target file
        :return normalized_channels.txt
        """

        with codecs.open(file_path, 'r', encoding='utf8') as fr:
            # remove punctuations
            punctuations = env_punc + chs_punc
            channels = [re.sub('[%s]' % punctuations, ' ', line.strip()) for line in fr]

            # remove channels including chinese garbled
            channels = [channel for channel in channels if not re.search('[^(\w+\-)]', channel)]

            # remove error channels
            channels = [channel for channel in channels if not re.match('^[a-zA-Z]+$', channel)]

            # remove Dolby、HD、高清 channels
            channels = [channel for channel in channels if not re.search('(Dolby|HD)$', channel)]
            channels = sorted(list(set(channels)))
            tmp_channels = []
            for channel in channels:
                if channel[-2:] != '高清':
                    tmp_channels.append(channel)
                elif channel[:-2] not in tmp_channels:
                    tmp_channels.append(channel)

            with codecs.open(TMP_PATH + '/normalized_channels.txt', 'w', encoding='utf') as fw:
                fw.write('\n'.join(sorted(set(channels))))

if __name__ == "__main__":
    handler = Preprocess()

    # handler.extract_all_events(3)
    # handler.get_all_channels_programs()
    # handler.normalize_programs(TMP_PATH + '/all_unique_programs.txt')
    # handler.normalize_channels(TMP_PATH + '/all_unique_channels.txt')
