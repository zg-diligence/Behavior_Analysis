import os
import re
import time
import codecs
import graphviz as gv
import multiprocessing
from matplotlib import pyplot as plt
from matplotlib import colors as mcolors
from collections import Counter, deque
from datetime import datetime, timedelta
from string import punctuation as env_punc
from zhon.hanzi import punctuation as chs_punc
plt.rcParams['font.sans-serif'] = ['FZKai-Z03']

DEBUG = True
ROOT_PATH = '/media/gzhang/Elements/original_data'
EXTRACT_PATH = '/media/gzhang/Data/user_data'

TMP_PATH = os.path.join(os.getcwd(), 'tmp_result')
GRAPH_PATH = os.path.join(TMP_PATH, 'user_graph')
EVENT_PATH = os.path.join(TMP_PATH, 'user_events')
PATTERN_PATH = os.path.join(TMP_PATH, 'user_pattern')
PREFER_PATH = os.path.join(TMP_PATH, 'prefer_analysis')

categories = ['电影', '电视剧', '新闻', '体育', '财经',
              '法制', '军事', '农业', '纪实', '音乐',
              '戏曲', '少儿', '健康', '时尚', '美食',
              '汽车', '旅游', '综艺', '科教', '生活',  # life is in the later
              '亲子', '购物', '电台', '其它']


def correct_program_dict(src_path, des_path):
    """
    correcting the classification result got from classifyer section
    :param src_path: src file path of original classification result
    :param des_path: des file path to write the corrected classification result
    :return:
    """

    with open(src_path) as fr:
        lines = [line.strip().split('\t\t') for line in fr.readlines()]
        prog_cat_src = [(prog, cat, src) for src, cat, prog in lines]

        others = ['NULL', '以实际播出内容为准', '以播出为准', '休息',
                  '结束', '播出结束', '休台', '专题节目', '节目预告',
                  '停机检修', '广告']

        # find duplicated programs
        dup_prog, single_dict = [], {}
        for prog, cat, src in prog_cat_src:
            if not single_dict.get(prog, 0):
                single_dict[prog] = (cat, src)
            else:
                if cat != single_dict[prog][0] and prog not in others:
                    dup_prog.append(prog)

        # collect all duplicated result
        dup_dict = dict(zip(set(dup_prog), [[] for _ in range(len(dup_prog))]))
        for prog, cat, src in prog_cat_src:
            if prog in dup_prog:
                dup_dict[prog].append((cat, src))

        # select the better category
        res_dict = {}
        for key, values in dup_dict.items():
            if len(values) == 2 and values[0][1] in ['short', 'post']:
                res_dict[key] = values[0][0] if values[0][1] == 'short' else values[1][0]
            else:
                cats = [cat for cat, src in values]
                res_dict[key] = sorted(dict(Counter(cats)).items(), key=lambda item: item[1], reverse=True)[0][0]

        # correct classification result
        prog_cat = [(a, b) for a, b, _ in prog_cat_src]
        prog_dict = dict(prog_cat)
        for key, value in res_dict.items():
            prog_dict[key] = value
        for key in others:
            prog_dict[key] = "其它"

        # write into destination file
        with open(des_path, 'w') as fw:
            for key, value in sorted(prog_dict.items(), key=lambda item: (item[1], item[0])):
                fw.write(value + '\t\t' + key + '\n')


def regex_for_normalize_programs():
    """
    regexies for normalizing programs
    :return: list
    """

    regexes = []
    chs_num = '一二三四五六七八九十'
    punctuations = env_punc + chs_punc
    unvisible_chars = ''.join([chr(i) for i in range(32)])

    #  # remove program marks
    regexes.append(re.compile('.*(报复|反复|回复|修复)$'))
    regexes.append(re.compile('复$'))
    regexes.append(re.compile('现场直播|实况录像|免费'))
    regexes.append(re.compile('(中文版|英文版|回看|复播|重播|[上中下尾]|[ⅡⅢI]+)$'))
    regexes.append(re.compile('^(HD|3D|杜比)|(SD文广版|文广(遮标)*版|SD|HD|3D|TV|杜比|限免|done|out)$'))

    # remove date
    r1 = '(19|20)*[0-9][0-9](年|-)(0[1-9]|10|11|12|[0-9])(月|-)(0[1-9]|[1-2][0-9]|3[0-1]|[0-9])日*'
    r2 = '(19|20)*[0-9][0-9](年|-)*(0[1-9]|10|11|12)(月|-)*(0[0-9]|[1-2][0-9]|3[0-1])日*\d*'
    r3 = '(0[1-9]|10|11|12|[1-9])月(0[1-9]|[1-2][0-9]|3[0-1]|[1-9])日'
    r4 = '(0[1-9]|10|11|12)(0[1-9]|[1-2][0-9]|3[0-1])'
    r7 = '(19|20)[0-9][0-9]-*\d*'
    r5 = '(19|20)[0-9][0-9]年'
    r6 = '(19|20)[0-9][0-9](-|/)(19|20)[0-9][0-9]'
    regexes.append(re.compile('(%s|%s|%s|%s|%s|%s|%s)' % (r1, r2, r3, r4, r5, r6, r7)))

    # remove space/punctuations/control chars
    regexes.append(re.compile('\s'))
    regexes.append(re.compile('[%s]' % punctuations))
    regexes.append(re.compile('[%s]' % unvisible_chars))

    # remove serial number
    regexes.append(re.compile('第*([%s]+|\d+)[期部季集]+' % chs_num))
    regexes.append(re.compile('(\d+|[%s]+)$' % chs_num))

    return regexes


def preprocess_program(program):
    """
    preprocess of program
    :param program:
    :return:
    """

    chs_num = '一二三四五六七八九十'
    regexes = regex_for_normalize_programs()

    # remove serial number in the middle of the program name
    if re.match('^\D+第([%s]+|\d+)[部集季]+.*$' % chs_num, program):
        res = re.search('第([%s]+|\d+)[部集季]+' % chs_num, program)
        program = program[:res.span()[0]]

    for regex in regexes[2:]:
        program = re.sub(regex, '', program)
    if not re.match(regexes[0], program):
        program = re.sub(regexes[1], '', program)

    for regex in regexes[2:]:
        program = re.sub(regex, '', program)
    if not re.match(regexes[0], program):
        program = re.sub(regexes[1], '', program)

    # remove chinese garbled
    if re.search('[^(\w+\-)]', program):
        return None

    return None if not program else program


class Recognizer(object):
    def __init__(self):
        self.programs_dict = None

    def count_usr_num(self, root_catelogue, folder, filenames):
        """
        count user appearance in a single folder
        :param root_catelogue: root catelogue of the source data
        :param folder: the given folder name
        :param filenames: name of the files in the folder
        :return:
        """

        user_ids = {}
        folder_path = os.path.join(root_catelogue, folder)
        for filename in filenames:
            file_path = os.path.join(folder_path, filename)
            for line in open(file_path):
                try:
                    usr_id = line.split('|')[3]
                except:
                    continue
                else:
                    if not usr_id: continue
                    if not user_ids.get(usr_id, 0):
                        user_ids[usr_id] = 1
                    else:
                        user_ids[usr_id] += 1
        return user_ids

    def choose_usrs(self, root_catelogue, processes_num=3, choose_num=50):
        """
        count users with the most appearances
        :param root_catelogue: root catelogue of the source data
        :param processes_num: number of the process
        :param choose_num: number of users to choose
        :return: write the first 50 ids into file
        """

        # choose days from 04 - 10
        folders = sorted(os.listdir(root_catelogue), key=lambda item: int(item))[2:9]
        all_filenames = []
        for folder in folders:
            folder_path = os.path.join(root_catelogue, folder)
            all_filenames.append(os.listdir(folder_path))

        processess = []
        pool = multiprocessing.Pool(processes_num)
        for folder, filenames in zip(folders, all_filenames):
            processess.append(pool.apply_async(self.count_usr_num, (root_catelogue, folder, filenames)))
        pool.close()
        pool.join()

        user_ids = Counter({})
        for p in processess:
            user_ids += Counter(p.get())

        choosed_usrs = sorted(user_ids.items(), key=lambda item: item[1], reverse=True)[:choose_num]
        choosed_usr_ids = [usr_id for usr_id, _ in choosed_usrs]
        with open(TMP_PATH + '/choosed_usrs.txt', 'w') as fw:
            fw.write('\n'.join(choosed_usr_ids))

        if DEBUG:
            for item in choosed_usrs: print(item)
            print(len(user_ids.keys()))

    def extract_usr_events(self, root_catelogue, usr_ids, folder):
        """
        extract target user events in one folder
        :param root_catelogue: root catelogue of source data
        :param usr_ids: id of target users
        :param folder: given folder
        :return: extracted events and name of the folder
        """

        folder_path = os.path.join(root_catelogue, folder)
        filenames = sorted(os.listdir(folder_path), key=lambda item: item[-10:-8])
        usr_events = dict(zip(usr_ids, [[] for _ in range(len(usr_ids))]))
        for filename in filenames:
            file_path = os.path.join(folder_path, filename)
            if DEBUG: print(file_path)
            for line in codecs.open(file_path, encoding='gb18030', errors='ignore'):
                try:
                    usr_id = line.split('|')[3]
                except:
                    continue
                else:
                    if usr_id not in usr_ids: continue
                    usr_events[usr_id].append(line.strip())

        # sorting events by random sequence and message ID
        for _, events in usr_events.items():
            events.sort(key=lambda item: (item.split('|')[2], int(item.split('|')[0])))

        return usr_events, folder

    def extract_all_usr_events(self, root_catelogue, des_catelogue, usr_ids, processes_num=3):
        """
        extract target user events in all folders
        :param root_catelogue: root catelogue of source data
        :param des_catelogue: root catelogue of destionation location
        :param usr_ids: id of target users
        :param processes_num: num of the processes
        :return:
        """

        folders = sorted(os.listdir(root_catelogue), key=lambda item: int(item))

        processes = []
        pool = multiprocessing.Pool(processes_num)
        for folder in folders:
            processes.append(pool.apply_async(self.extract_usr_events, (root_catelogue, usr_ids, folder)))
        pool.close()
        pool.join()

        all_usr_events = [p.get() for p in processes]
        all_usr_events = sorted(all_usr_events, key=lambda item: int(item[1]))
        all_usr_events = [item[0] for item in all_usr_events]
        for usr_events, folder in zip(all_usr_events, folders):
            folder_path = os.path.join(des_catelogue, folder)
            if not os.path.exists(folder_path):
                os.mkdir(folder_path)

            for usr_id, events in usr_events.items():
                file_path = os.path.join(folder_path, usr_id)
                with codecs.open(file_path, mode='w', encoding='utf8') as fw:
                    fw.write('\n'.join(events))

    def adjust_extracted_events(self, root_catelogue, des_catelogue):
        """
        adjust order and format of extracted events
        :param root_catelogue:
        :param des_catelogue:
        :return:
        """

        folders = sorted(os.listdir(root_catelogue), key=lambda item: int(item))
        for folder in folders:
            src_folder_path = os.path.join(root_catelogue, folder)
            des_folder_path = os.path.join(des_catelogue, folder)
            if not os.path.exists(des_folder_path):
                os.mkdir(des_folder_path)

            filenames = sorted(os.listdir(src_folder_path))
            for filename in filenames[:49]:
                src_file_path = os.path.join(src_folder_path, filename)
                des_file_path = os.path.join(des_folder_path, filename)
                with open(src_file_path) as fr:
                    events = [line.strip() for line in fr.readlines()]
                    events.sort(key=lambda item: (item.split('|')[2], int(item.split('|')[0])))

                    cycles, cycle = [], events[:1]
                    if cycle:
                        pre, cnt = events[0].split('|'), 1
                        for event in events[1:]:
                            cur = event.split('|')
                            if cur[1] == '20': continue  # remove heartbeat event
                            if cur[2] != pre[2]:
                                if cycle[0].split('|')[1] == '20': cycle.pop(0)
                                if len(cycle) >= 3: cycles.append(cycle)
                                cycle, cnt = [], 0
                            cycle.append(event)
                            pre, cnt = cur, cnt + 1

                        # only one cycle in a single file
                        if cycle[0].split('|')[1] == '20': cycle.pop(0)
                        if len(cycle) >= 3: cycles.append(cycle)

                    # sorting cycles by time
                    cycles.sort(key=lambda cycle: datetime.strptime(cycle[0].split('|')[5], '%Y.%m.%d %H:%M:%S'))

                    new_events = ['\n'.join(cycle) for cycle in cycles]
                    with open(des_file_path, 'w') as fw:
                        fw.write('\n\n'.join(new_events))


    def count_edges(self, file_path):
        """
        count edges in a single file by cycle
        :param file_path: path of the given file
        :return: dict, edges and relative numbers
        """

        events = [line.strip() for line in open(file_path).readlines() if line != '\n']
        if not events: return {}

        edges = {}
        pre = events[0].split('|')
        for event in events[1:]:
            cur = event.split('|')
            if cur[2] == pre[2]:
                edge = (pre[1], cur[1])
                if not edges.get(edge, 0):
                    edges[edge] = 1
                else:
                    edges[edge] += 1
            pre = cur
        return edges

    def count_edges_by_day(self, root_catelogue, folder, usr_ids):
        """
        count edges in one day of all users
        :param root_catelogue:
        :param folder:
        :param usr_ids:
        :return:
        """

        folder_path = os.path.join(root_catelogue, folder)

        usr_edges = dict(zip(usr_ids, [[] for _ in range(len(usr_ids))]))
        for usr_id in usr_ids:
            file_path = os.path.join(folder_path, usr_id)
            edges = self.count_edges(file_path)
            usr_edges[usr_id] = edges
        return usr_edges

    def count_all_edges(self, root_catelogue, usr_ids, days=3):
        """
        count all edges among the given days
        :param root_catelogue:
        :param usr_ids:
        :param days: number of days to count
        :return:
        """

        folders = sorted(os.listdir(root_catelogue), key=lambda item: int(item))[:days]
        all_usr_edges = [self.count_edges_by_day(root_catelogue, folder, usr_ids) for folder in folders]
        res_usr_edges = dict(zip(usr_ids, [Counter() for _ in range(len(usr_ids))]))
        for usr_edges in all_usr_edges:
            for usr_id, edges in usr_edges.items():
                res_usr_edges[usr_id] += Counter(edges)

        if DEBUG:
            for usr_id, edges in res_usr_edges.items():
                print(usr_id, sorted(edges.items(), key=lambda item: item[1], reverse=True))

        return res_usr_edges

    def display_usr_graphs(self, usr_edges, limit=200):
        """
        display directed graph of user pattern
        :param usr_edges:dict usr_id and edges
        :param limit: minimal weight of the edges to display in the graph
        :return:
        """

        # compute total weights of edges
        total_edges = Counter()
        for edges in usr_edges.values():
            total_edges += Counter(edges)
        usr_edges['total_usrs'] = total_edges

        for usr_id, edges in usr_edges.items():
            pic_path = os.path.join(GRAPH_PATH, usr_id)
            g_edges = [(edge[0], edge[1], str(num)) for edge, num in edges.items() if num >= limit]
            graph = gv.Digraph(format='png')
            for edge in g_edges:
                graph.edge(*edge)
            graph.render(pic_path)
            os.system('rm ' + pic_path)


    def read_programs_dict(self, file_path):
        """
        read the program classification result
        :param file_path: path of the source file
        :return:
        """

        with open(file_path) as fr:
            cat_progs = [line.strip().split('\t\t') for line in fr.readlines()]
            prog_cats = [(prog, cat) for cat, prog in cat_progs]
        self.programs_dict = dict(prog_cats)

    def classify_program(self, program):
        """
        classify program by exist programs classification result
        :param program: un-normalized program
        :return: category of the program
        """

        if re.match('^\d+-.*-.*$', program):
            return '音乐'
        else:
            new_prog = preprocess_program(program)
            if not self.programs_dict.get(new_prog, 0): return None
            return self.programs_dict[new_prog]

    def handle_stack(self, stack, event_id, threshold):
        """
        extract behaviors 17/96/97
        :param stack:
        :param event_id: flag for current behavior
        :param threshold: time threshold
        :return:
        """

        tmp_lst = deque()
        while stack and stack[-1][1] == event_id:
            tmp_lst.appendleft(stack.pop())

        start_time = datetime.strptime(tmp_lst[0][5], '%Y.%m.%d %H:%M:%S')
        end_time = datetime.strptime(tmp_lst[-1][5], '%Y.%m.%d %H:%M:%S')
        last_time = end_time - start_time
        if last_time < threshold: return None

        program = tmp_lst[0][10] if event_id in ['17', '97'] else tmp_lst[0][-2]
        category = self.classify_program(program)
        category = category if category else '其它'
        pattern = {'17': 'time-shift', '96': 'vod-play', '97': 'time-shift'}[event_id]
        return pattern, tmp_lst[0][5], tmp_lst[-1][5], last_time, category, program

    def recognize_pattern(self, circle, threshold=timedelta(seconds=30)):
        """
        recognize behavior patters 5/17/96/97
        :param circle: a consecutive sequence
        :param threshold: time threshold for one behavior
        :return: list of pattern tuples
        """

        events = [event.split('|') for event in circle]
        events.append([None, None])

        patterns, stack = [], []
        for event in events[1:]:
            if stack and stack[-1][1] != event[1]:
                if stack[-1][1] == '5':
                    start_time = datetime.strptime(stack[-1][6], '%Y.%m.%d %H:%M:%S')
                    end_time = datetime.strptime(stack[-1][5], '%Y.%m.%d %H:%M:%S')
                    last_time = end_time - start_time
                    if last_time < threshold: continue

                    program = stack[-1][11]
                    category = self.classify_program(program)
                    category = category if category else '其它'
                    patterns.append(('look-through', stack[-1][6], stack[-1][5], last_time, category, program))
                elif stack[-1][1] == '17':
                    pattern = self.handle_stack(stack, '17', threshold)
                    if pattern: patterns.append(pattern)
                elif stack[-1][1] == '96':
                    pattern = self.handle_stack(stack, '96', threshold)
                    if pattern: patterns.append(pattern)
                elif stack[-1][1] == '97':
                    pattern = self.handle_stack(stack, '97', threshold)
                    if pattern: patterns.append(pattern)
                stack = []
            else:
                stack.append(event)
        return patterns

    def recognize_pattern_by_user(self, usr_files):
        """
        recognize all behaviors of one user for the whole month
        :param usr_files: paths of all source files
        :return: list  of all pattern tuples
        """

        patterns = []
        for filepath in usr_files:
            cycles = open(filepath).read().split('\n\n')
            cycles = [cycle.split('\n') for cycle in cycles]
            for cycle in cycles:
                patterns += self.recognize_pattern(cycle)
        return patterns

    def recognize_pattern_all_usrs(self, root_catelogue, des_catelogue, usr_ids):
        """
        recognize user behavior patterns
        :param root_catelogue: root catelogue of source data
        :param des_catelogue: folder path for writting user behaviors
        :param usr_ids: id of all selected users
        :return: write all user behaviors into file by user
        """

        folders = sorted(os.listdir(root_catelogue), key=lambda item: int(item))

        # classify files by usr
        files_by_usr = [[] for _ in range(49)]
        for folder in folders:
            folder_path = os.path.join(root_catelogue, folder)
            filenames = sorted(os.listdir(folder_path))
            for i in range(len(filenames)):
                files_by_usr[i].append(os.path.join(folder_path, filenames[i]))

        for usr_id, usr_files in zip(usr_ids, files_by_usr):
            patterns = self.recognize_pattern_by_user(usr_files)
            cat_time = self.analyze_preference(patterns)
            self.display_usr_preference(usr_id, cat_time)
            file_path = os.path.join(des_catelogue, usr_id)
            with open(file_path, 'w') as fw:
                for pattern in patterns:
                    tmp_pat = list(pattern)
                    tmp_pat[3] = str(pattern[3])
                    fw.write(str(tmp_pat) + '\n')


    def analyze_preference(self, patterns):
        """
        count total time by category
        :param patterns:
        :return: list of (category, time) pair
        """

        cat_time = dict(zip(categories, [timedelta(seconds=0) for _ in range(len(categories))]))
        for pattern in patterns:
            cat_time[pattern[4]] += pattern[3]
        return sorted(cat_time.items(), key=lambda item: item[1].seconds, reverse=True)

    def display_usr_preference(self, usr_id, cat_time):
        """
        Visualization of user preference
        :param usr_id:
        :param cat_time: list of (category, time) pair
        :return:
        """

        times = [t.seconds for _, t in cat_time]
        portion = [t / sum(times) for t in times]
        prefer_portion = portion[:10] + [1 - sum(portion[:10]), ]  # choose the first 10 categories
        if DEBUG: print(sum(prefer_portion[:5]), sum(prefer_portion[:10]), prefer_portion)

        labels = [cat for cat, _ in cat_time[:10]] + ['其它', ]
        colors = dict(mcolors.BASE_COLORS, **mcolors.CSS4_COLORS)
        colors = sorted([item for item in colors.values() if type(item) != tuple])
        colors = [colors[1 + i * 13] for i in range(len(labels))]

        plt.figure(figsize=(8, 6))
        plt.pie(prefer_portion, labels=labels, colors=colors,
                autopct='%3.1f%%', shadow=False, startangle=90)
        plt.title(usr_id)
        plt.axis('equal')
        plt.savefig(PREFER_PATH + '/' + usr_id + '.png')


if __name__ == '__main__':
    if DEBUG: print(time.strftime("%Y-%m-%d %X", time.localtime()))

    if not os.path.exists(EXTRACT_PATH):
        os.mkdir(EXTRACT_PATH)
    if not os.path.exists(GRAPH_PATH):
        os.mkdir(GRAPH_PATH)
    if not os.path.exists(EVENT_PATH):
        os.mkdir(EVENT_PATH)
    if not os.path.exists(PATTERN_PATH):
        os.mkdir(PATTERN_PATH)
    if not os.path.exists(PREFER_PATH):
        os.mkdir(PREFER_PATH)

    handler = Recognizer()
    # handler.choose_usrs(ROOT_PATH)
    usr_ids = sorted([line.strip() for line in open(TMP_PATH + '/choosed_usrs.txt')])

    # handler.extract_all_usr_events(ROOT_PATH, EXTRACT_PATH, usr_ids, processes_num=4)
    # handler.adjust_extracted_events(EXTRACT_PATH, EVENT_PATH)

    # usr_edges = handler.count_all_edges(EVENT_PATH, usr_ids, days=28)
    # handler.display_usr_graphs(usr_edges, limit=1500)

    handler.read_programs_dict(TMP_PATH + '/nprograms_dict.txt')
    handler.recognize_pattern_all_usrs(EVENT_PATH, PATTERN_PATH, usr_ids)

    if DEBUG: print(time.strftime("%Y-%m-%d %X", time.localtime()))
