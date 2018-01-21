# -*- coding:utf-8 -*-

import PathMaker
import os
import logging
#import TrieTree

def mycmp(x,y):
    x = x.split('|')
    y = y.split('|')
    if cmp(x[2],y[2])==0:
        return int(x[0])-int(y[0])
    return cmp(x[2],y[2])

#trie = TrieTree.Trie()
ServerNo = [1,2,3,4,5,7,9,10,11,12,1,1,2,2,3,3,4,4,5,5,7,7,9,9,9,10,10,10,11,11,11,12,12]
ServerMin = [06,39,59,06,31,41,49,56,9,48,16,46,21,51,36,16,46,31,57,47,56,45,01,06,45,06,16,45,11,36,45,31,11]
ServerSec = [16,53,32,51,59,21,46,12,11,36,54,55,55,55,54,54,54,55,29,29,53,44,56,56,47,54,54,45,51,52,42,55,55]
# TargetUser = ['825010354067360','825010214580868','825010269689626','825010213844101','825010373957410','825010213836345','825010213880008','825010367831984',]
ed_TargetUser = ['825010278646143','825010213832088','825010385953672','825010213892153','825010278655112','825010278620036','825010226259155','825010226809731',
'825010228233866','825010213801266','825010373952909','825010253081685','825010212925883','825010228331695','825010226233260','825010228330972','825010257108005',
'825010226967702','825010269608391','825010226215141','825010228356599','825010213886096','825010226200454','825010226861145','825010269685604','825010251163333',]
TargetUser = []
fp = open('UserList.txt','r')
cnt = len(ed_TargetUser)
thresh = 500
for line in fp.xreadlines():
    user = line.decode('gbk').strip()
    if user not in ed_TargetUser:
        TargetUser.append(user)
        cnt += 1
    if cnt>=thresh:
        break
    
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(funcName)s[line:%(lineno)d][%(levelname)s]: %(message)s',
                    datefmt='[%Y-%m-%d %H:%M:%S]',
                    filename='CA_SelectUser.log',
                    filemode='w')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


for i_day in range(3,32):#range(1,2):
    # dic = {}
    for i_hour in range(0,24):
        dic = {}
        for i in range(len(ServerNo)):
            date = PathMaker.NTADate(i_day,5,2016)
            time = PathMaker.NTATime(i_hour,ServerMin[i],ServerSec[i])
            path = PathMaker.make_in_path(ServerNo[i],date,time)
            # print path
            logging.info(path)
            try:
                fp = open(path, 'r')
                fp.readline()
                # t = 0
                for line in fp:
                    # t += 1
                    try:
                        tmp = line.decode('gbk').encode('utf8').strip().split('|')
                    #except UnicodeDecodeError:
                    #    continue
                    #try:
                        key = tmp[3]
                        # event = tmp[1]
                        '''
                        find = trie.search(key)
                        if find is None:
                            find = trie.insert(key)
                        #trie.count(find, event)
                        trie.count_line(find, line)
                        '''
                        if key not in TargetUser:
                            continue
                        if key not in dic:
                            dic[key] = [line]
                        else:
                            dic[key].append(line)
                    except:
                        continue
                fp.close()
            except IOError as e:
                logging.error(e)
                # print 'can\'t open file:'+path


        #trie.classify(str(i_day),str(i_hour))

        #outpath =  "count/"+ str(i_day) + "_" + str(i_hour)+".txt"
        #fp = open(outpath,'w')
        #trie.display(fp)
        #fp.close()
    # print 'total:',len(dic)

    # base_dir = os.path.dirname(__file__)
    # outpath = os.path.join(base_dir, "CACardNO_Day",str(i_day))
    # if not os.path.exists(outpath):
    #     os.makedirs(outpath)

    # for key,lists in dic.items():
    #     #print outpath
    #     fp = open(outpath + '/'+key+'.txt','a')
    #     lists.sort(mycmp)
    #     for i in lists:
    #         fp.write(i)
    #     fp.close()

        base_dir = os.path.dirname(__file__)
        outpath = os.path.join(base_dir, "CACardNO_Day",str(i_day))
        if not os.path.exists(outpath):
            os.makedirs(outpath)

        for key,lists in dic.items():
            #print outpath
            fp = open(outpath + '/'+key+'.txt','a')
            lists.sort(mycmp)
            for i in lists:
                fp.write(i)
            fp.close()