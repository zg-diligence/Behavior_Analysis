# -*-coding:utf-8-*-

import pymysql
import os
import datetime

starttime = datetime.datetime.now()

class NTADate:
    def __init__(self, nDay=1, nMonth=1, nYear=2016):
        self.Day = "%02d" % nDay
        self.Month = "%02d" % nMonth
        self.Year = str(nYear)
        self.Date = self.Year + self.Month + self.Day


class NTATime:
    def __init__(self, nHour, nMin, nSec):
        self.Hour = "%02d" % nHour
        self.Min = "%02d" % nMin
        self.Sec = "%02d" % nSec
        self.Time = self.Hour + self.Min + self.Sec


def make_in_path(ServerNo, Date, Time):
    BASE_DIR = os.path.dirname(__file__)
    server = "Server%02d" % ServerNo
    file_path  = os.path.join(BASE_DIR,"dataset",Date.Day,server + "_" + Date.Date + Time.Time + ".dat")
    #return "\\dataset\\" + Date.Day + "\\" + server + "_" + Date.Date + Time.Time + ".dat"
    return file_path


db = pymysql.connect(
    host='127.0.0.1',
    port=3306,
    user='root',
    password='',
    db='tvdata',
    charset='utf8'
)

ServerNo = 3
for i in range(19,24):
    date = NTADate(1,5,2016)
    time = NTATime(i,59,32)
    path = make_in_path(ServerNo, date, time)
    #fp = codecs.open(path, "r","gb2312")
    print datetime.datetime.now(),
    print path
    fp = open(path,"r")
    if fp:
        fp.readline()
        #cnt = 1

        for line in fp:
            #cnt += 1
            #print cnt,
            try:
                #print line.decode("gbk").encode("utf8")
                tmp = line.decode("gbk").encode("utf8").strip().split('|')
            except UnicodeDecodeError:
                continue
            try:
                with db.cursor() as cursor:
                    if tmp[1]=='21':
                        values = tmp[0:6]
                        values.append(tmp[-1])
                        values[1] = int(values[1])
                        values.append(ServerNo)
                        values = tuple(values)
                        sql = "INSERT IGNORE INTO user(CACardNO) VALUES(%s)"
                        cursor.execute(sql,(values[3]))

                        sql = "INSERT IGNORE INTO idandseq(MessageID,EventID,RandomSeq,CACardNO,Seq,Time,ServerTime,ServerNO) " \
                              "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
                        cursor.execute(sql, values)
                        if int(cursor.rowcount)==0:
                            continue
                        cursor.execute("SELECT last_insert_id()")
                        t_id = cursor.fetchone()[0]
                        values = tmp[6:-2]
                        values[2] = int(values[2])
                        values[-3:] = map(int,values[-3:])
                        values.append(t_id)
                        values = tuple(values)
                        sql = "INSERT IGNORE INTO serviceandprogram" \
                              "(ServiceID,TSID,FreqPt,ChannelName,ProgramName,Authorization,SignalIntensity,SignalQuality,id)" \
                              "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        cursor.execute(sql,values)

                        sql = "INSERT IGNORE INTO `21|channelinevent`(SDVProgram,id)" \
                              "VALUES(%s,%s)"
                        cursor.execute(sql,(int(tmp[-2]),t_id))

                    elif tmp[1]=='5':
                        values = tmp[0:5]
                        values.append(tmp[-1])
                        values[1] = int(values[1])
                        values.append(ServerNo)
                        values = tuple(values)
                        sql = "INSERT IGNORE INTO user(CACardNO) VALUES(%s)"
                        cursor.execute(sql,(values[3]))

                        sql = "INSERT IGNORE INTO idandseq(MessageID,EventID,RandomSeq,CACardNO,Seq,ServerTime,ServerNo) " \
                              "VALUES(%s,%s,%s,%s,%s,%s,%s)"
                        cursor.execute(sql, values)
                        if int(cursor.rowcount)==0:
                            continue

                        cursor.execute("SELECT last_insert_id()")
                        t_id = cursor.fetchone()[0]

                        values = tmp[7:-3]
                        values[2] = int(values[2])
                        values[-3:] = map(int, values[-3:])
                        values.append(t_id)
                        values = tuple(values)
                        sql = "INSERT IGNORE INTO serviceandprogram" \
                              "(ServiceID,TSID,FreqPt,ChannelName,ProgramName,Authorization,SignalIntensity,SignalQuality,id)" \
                              "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        cursor.execute(sql, values)

                        values = tmp[5:7]
                        values.extend(map(int,tmp[-3:-1]))
                        values.append(t_id)
                        values = tuple(values)
                        sql = "INSERT IGNORE INTO `5|channelexitevent`(EndTime,BeginTime,SDVProgram,Duration,id)" \
                              "VALUES(%s,%s,%s,%s,%s)"
                        cursor.execute(sql,values)

            except:
                continue
    fp.close()
    db.commit()
db.close()
endtime = datetime.datetime.now()

print endtime-starttime
