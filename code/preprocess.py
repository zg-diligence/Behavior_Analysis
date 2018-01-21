import os, time
import re, codecs
import multiprocessing
from bs4 import BeautifulSoup
from urllib.request import urlopen, quote
DEBUG = True

class Preprocess:
    def __init__(self):
        self.events = ['21', '5', '96', '97', '6', '26', '24']

    def extract_events(self, src_folder, des_folder):
        """
        提取单个文件夹下events事件
        :param src_folder:源数据文件夹
        :param des_folder:目的数据文件夹
        """

        src_files = os.listdir(src_folder)
        err_file = 'extract_error/' + src_folder[-2:] + '_error.txt'
        with codecs.open(err_file, 'w', encoding='utf8') as err:
            for file_name in src_files:
                fr_path = src_folder + "/" + file_name
                fw_path = des_folder + "/" + file_name[:-3] + "txt"
                fr = codecs.open(fr_path, 'r', encoding='gb18030', errors='replace')
                fw = codecs.open(fw_path, 'w', encoding='utf8', errors='replace')
                for line in fr:
                    try:
                        if line.strip().split('|')[1] in self.events:
                            fw.write(line)
                    except IndexError as e: # invalid data item
                        err.write(fr_path + '|' + str(e) + '|' + line + '\n')
                fr.close()
                fw.close()

    def extract_all_events(self, root_catelogue, process):
        """
        提取所有文件夹下events事件
        :param root_catelogue:数据根目录
        :param process:进程数
        """

        src_catelogue = root_catelogue + "/original_data"
        des_catelogue = root_catelogue + "/zip_data"
        pool = multiprocessing.Pool(process)
        src_folders = os.listdir(src_catelogue)
        for folder in src_folders:
            src_folder = src_catelogue + "/" + folder
            des_folder = des_catelogue + "/" + folder
            if DEBUG: print("enter,", src_folder)
            if not os.path.exists(des_folder):
                os.mkdir(des_folder)
            pool.apply_async(self.extract_events, (src_folder, des_folder))
        pool.close()
        pool.join()

    def count_programs(self, src_folder):
        """
        提取单个文件夹下事件5,96,97中的频道和节目
        :param src_folder:源数据文件夹目录
        :return:事件5,96,97中的频道和节目
        """

        src_files = os.listdir(src_folder)
        results = [[] for _ in range(3)]
        err_file = 'count_error/' + src_folder[-2:] + '_error.txt'
        with codecs.open(err_file, 'w', encoding='utf8') as err:
            for filename in src_files:
                fr_path = src_folder + "/" + filename
                with codecs.open(fr_path, 'r', encoding='utf8', errors='replace') as fr:
                    for line in fr:
                        try:
                            items = line.strip().split('|')
                            if items[1] == "5":
                                results[0].append(items[10] + "|" + items[11])
                            elif items[1] == "96":
                                results[1].append(items[-2])
                            elif items[1] == "97":
                                results[2].append(items[9] + "|" + items[10])
                        except IndexError: # invalid data item
                            err.write(fr_path + "|"  + line +'\n')
        for i in range(len(results)):
            path = 'count_result/' + src_folder[-2:] + '_' + str(i) + '.txt'
            with codecs.open(path, 'w', encoding='utf8') as fw:
                fw.write('\n'.join(set(results[i])))

    def count_all_programs(self, catelogue, process):
        """
        提取所有文件夹下事件5, 96, 97中的频道和节目
        :param catelogue:源数据根目录
        :param process:进程数
        """

        folders = os.listdir(catelogue)
        pool = multiprocessing.Pool(process)
        for folder in folders:
            path = catelogue + '/' + folder
            if DEBUG: print("enter,", path)
            pool.apply_async(self.count_programs, (path,))
        pool.close()
        pool.join()

    def get_channels_programs(self, file_path):
        """
        提取文件中的电视频道和节目
        :param file_path:文件路径
        :return:频道和节目的有序列表
        """

        channels, programs = [], []
        with codecs.open(file_path, 'r', encoding='utf8') as fr:
            for line in fr:
                channel_program = line.strip().split('|')
                if channel_program[0]:
                    channels.append(channel_program[0])
                if channel_program[1]:
                    programs.append(channel_program[1])
        return list(set(channels)), list(set(programs))

    def normalized_programs(self, file_path):
        """
        规范化节目名称, 除去无用标识符
        :param file_path:文件路径
        :return 简化节目名称列表
        """

        programs, regexs = [], []
        regexs.append(re.compile('\(\w播\)|\(复\)|[-_][上中下尾]|第[一二三四五六七八九十]+[季集]'))
        regexs.append(re.compile('\(\d+-\d+\)$|（\d+-\d+）$|\d+-\d+$|\(\d+\)|（\d+）|\d+$'))
        regexs.append(re.compile('[\d+年]*\d+月\d+日|^20\d\d'))
        regexs.append(re.compile('[《》（）]'))
        regexs.append(re.compile('^\w+剧场[一二三四五六七八九十]*[:：]'))
        keywords = 'NBA|CBA|英超|中超|中甲|意甲|德甲|斯诺克|锦标赛|世界杯|' \
                   '游泳|射箭|田径|羽毛球|拳击|篮球|皮划艇|自行车|马术|击剑|足球|体操|蹦床|' \
                   '手球|曲棍球|柔道|现代五项|赛艇|帆船|射击|乒乓球|跆拳道|网球|铁人三项|排球|' \
                   '举重|摔跤|棒球|垒球|保龄球|台球|板球|体育舞蹈|壁球|武术|围棋|象棋|藤球|' \
                   '橄榄球|轮滑|空手道|卡巴迪|高尔夫球|龙舟'
        with codecs.open(file_path, 'r', encoding='utf8') as fr:
            for line in fr:
                tmp = line.strip()
                for regex in regexs:                    # 去除无关标识
                    tmp = re.sub(regex, '', tmp, re.I)
                if re.search(keywords, tmp, re.I):      # 体育类
                    tmp = '体育'
                programs.append(tmp.replace(' ', ''))   # 去除空格
        return sorted(set(programs))

    def normalize_chanels(self, path):
        """
        规范化 简化频道名称
        :param path: 文件路径
        :return: 规范化频道名称
        """

        with codecs.open(path, 'r', encoding='utf8') as fr:
            # 去除杂乱字符
            chanels = [re.sub('[;:：+# ]+', '', line.strip(), re.I) for line in fr]

            # 去除高清频道
            chanels = [chanel for chanel in chanels if not re.search('(Dolby|HD)$', chanel, re.I)]
            chanels = sorted(list(set(chanels)))
            tmp_chanels = []
            for chanel in chanels:
                if chanel[-2:] != '高清': tmp_chanels.append(chanel)
                elif chanel[:-2] not in tmp_chanels: tmp_chanels.append(chanel)

            # 去除中文乱码
            return [chanel for chanel in tmp_chanels if not re.search('[^(\w+\-)]', chanel, re.I)]

    def classify_chanels(self, path):
        """
        对频道进行大致分类
        :param path:文件路径
        """

        with codecs.open(path, 'r', encoding='utf8') as fr:
            regexs = ['^CCTV', '卫视$', '广播|电台|FM$|AM$', '新闻|资讯', '购物', '音乐', '影院|影视|电影', '\w+']
            chanels = [line.strip() for line in fr]
            categories = [[] for _ in range(len(regexs) + 1)]
            for chanel in chanels:
                for i in range(len(regexs)):
                    if re.search(regexs[i], chanel, re.I):
                        categories[i].append(chanel);break
            with codecs.open("files/classified_chanels.txt", 'w', encoding='utf8') as fw:
                for i in range(len(categories)):
                    fw.write('\n'.join(sorted(set(categories[i]))))
                    fw.write('\n' * 2)

    def scrapy_programs(self, program):
        """
        爬取百度百科节目简介
        :param program:节目名称
        :return:简介内容或error
        """

        url = 'https://baike.baidu.com/item/' + quote(program)
        html = urlopen(url)
        bsObj = BeautifulSoup(html, 'html.parser')
        if bsObj.head.title.get_text() == '百度百科——全球最大中文百科全书':
            return 'error'
        return bsObj.head.find_all('meta')


if __name__ == "__main__":
    handler = Preprocess()
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    # # 统计所有的频道和节目
    # catelogue = os.path.dirname(os.getcwd()) + "/zip_data"
    # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    # handler.count_all_programs(catelogue, 4)
    # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    # # 分别提取频道和节目
    # path = os.getcwd() + "/files/uniq_0.txt"
    # chanels_1, programs_1 = handler.get_channels_programs(path)
    # path = os.getcwd() + "/files/uniq_2.txt"
    # chanels_2, programs_2 = handler.get_channels_programs(path)
    # with codecs.open('files/extract_chanels.txt', 'w', encoding='utf8') as fw:
    #     fw.write('\n'.join(sorted(set(chanels_1 + chanels_2))))
    # with codecs.open('files/extract_programs.txt', 'w', encoding='utf8') as fw:
    #     fw.write('\n'.join(sorted(set(programs_1 + programs_2))))

    # # 规范化频道名称
    # normalized_chanels = handler.normalize_chanels('files/extract_chanels.txt')
    # with codecs.open('files/normalized_chanels.txt', 'w', encoding='utf8') as fw:
    #     fw.write('\n'.join(normalized_chanels))

    # # 简化节目名称
    # normalized_programs = handler.normalized_programs('files/extract_programs.txt')
    # with codecs.open('files/normalized_programs.txt', 'w', encoding='utf8') as fw:
    #     fw.write('\n'.join(normalized_programs))

    # 对频道名称进行简单分类
    # handler.classify_chanels('files/normalized_chanels.txt')

    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
