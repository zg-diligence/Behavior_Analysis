# -*- coding:utf-8 -*-

import PathMaker
import os
from multiprocessing import Pool

THREAD_NUM = 2
ServerNo = [1,2,3,4,5,7,9,10,11,12,1,1,2,2,3,3,4,4,5,5,7,7,9,9,9,10,10,10,11,11,11,12,12]
ServerMin = [06,39,59,06,31,41,49,56,9,48,16,46,21,51,36,16,46,31,57,47,56,45,01,06,45,06,16,45,11,36,45,31,11]
ServerSec = [16,53,32,51,59,21,46,12,11,36,54,55,55,55,54,54,54,55,29,29,53,44,56,56,47,54,54,45,51,52,42,55,55]

def mycmp(x,y):
    x = x.split('|')
    y = y.split('|')
    if cmp(x[2],y[2])==0:
        return int(x[0])-int(y[0])
    return cmp(x[2],y[2])

def _deal_file(path):
    try:
        print 'open file:'+path
        with open(path, 'r') as fp:
            pool = Pool(THREAD_NUM)
            sub_dic = pool.map(_deal_line, fp.xreadlines())
            pool.close()
            pool.join() 
    except IOError:
        print 'can\'t open file:'+path
        return {}

    sum_dic = {}
    for item in sub_dic:
        if item is None:
            continue
        if item[0] not in sum_dic:
            sum_dic[item[0]] = item[1]
        else:
            sum_dic[item[0]] += item[1]
    return sum_dic

def _deal_line(line):
    try:
        args_list = line.decode('gbk').encode('utf8').strip().split('|')
        key = args_list[3]
        # if key not in dic:
        #     dic[key] = [line]
        # else:
        #     dic[key].append(line)
    except:
        return None
    # return dic
    return (key,[line])

def main():

    for i_day in range(1,2):
        for i_hour in range(0,1):
            sum_dic = {}
            for i in range(len(ServerNo)):
                date = PathMaker.NTADate(i_day,5,2016)
                time = PathMaker.NTATime(i_hour,ServerMin[i],ServerSec[i])
                path = PathMaker.make_in_path(ServerNo[i],date,time)
                file_dic = _deal_file(path)
                # sum_counter += counter
                for key,value in file_dic.items():
                    if key not in sum_dic:
                        sum_dic[key] = value
                    else:
                        sum_dic[key] += value

            print 'total:',len(sum_dic)

            base_dir = os.path.dirname(__file__)
            outpath = os.path.join(base_dir, "CACardNO",str(i_day)+"_"+str(i_hour))
            if not os.path.exists(outpath):
                os.makedirs(outpath)

            for key,lists in sum_dic.items():
                #print outpath
                try:
                    fp = open(outpath + '\\'+key+'.txt','w')
                    lists.sort(mycmp)
                    for i in lists:
                        fp.write(i)
                    fp.close()
                except TypeError:
                    print 'TypeError:'+outpath+'\\'+key+'.txt'

if __name__ == '__main__':

    main()


# dic = {}
# for i_day in range(1,2):
#     for i_hour in [18,23]:#range(22,23):
#         for i in range(len(ServerNo)):
#             date = PathMaker.NTADate(i_day,5,2016)
#             time = PathMaker.NTATime(i_hour,ServerMin[i],ServerSec[i])
#             path = PathMaker.make_in_path(ServerNo[i],date,time)
#             print path
#             try:
#                 fp = open(path, 'r')
#                 fp.readline()
#                 t = 0
#                 for line in fp:
#                     t += 1
#                     try:
#                         tmp = line.decode('gbk').encode('utf8').strip().split('|')
#                     #except UnicodeDecodeError:
#                     #    continue
#                     #try:
#                         key = tmp[3]
#                         event = tmp[1]
#                         '''
#                         find = trie.search(key)
#                         if find is None:
#                             find = trie.insert(key)
#                         #trie.count(find, event)
#                         trie.count_line(find, line)
#                         '''
#                         if key not in dic:
#                             dic[key] = [line]
#                         else:
#                             dic[key].append(line)
#                     except:
#                         continue
#                 fp.close()
#             except IOError:
#                 print 'can\'t open file:'+path


#         #trie.classify(str(i_day),str(i_hour))

#         #outpath =  "count/"+ str(i_day) + "_" + str(i_hour)+".txt"
#         #fp = open(outpath,'w')
#         #trie.display(fp)
#         #fp.close()
#         print 'total:',len(dic)

#         base_dir = os.path.dirname(__file__)
#         outpath = os.path.join(base_dir, "CACardNO",str(i_day)+"_"+str(i_hour))
#         if not os.path.exists(outpath):
#             os.makedirs(outpath)

#         for key,lists in dic.items():
#             #print outpath
#             fp = open(outpath + '/'+key+'.txt','w')
#             lists.sort(mycmp)
#             for i in lists:
#                 fp.write(i)
#             fp.close()