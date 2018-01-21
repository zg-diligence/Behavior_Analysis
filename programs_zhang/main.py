# -*-coding:utf-8-*-

#import MySQLdb
import pymysql
import os
from time import clock
import logging
from multiprocessing import Pool

from models import NTADate, NTATime, ChannelEnterEvent, ChannelExitEvent

#PROCESSING_NUMBER = 8
PROCESSING_NUMBER = 4

# db config
HOST = "127.0.0.1"
PORT = 3306
USERNAME = 'root'
PASSWORD = '12345678'
DB = 'tvdata'

# log config

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(funcName)s[line:%(lineno)d][%(levelname)s]: %(message)s',
                    datefmt='[%Y-%m-%d %H:%M:%S]',
                    filename='input_database.log',
                    filemode='w')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def make_in_path(server_no, date, time):
    base_dir = os.path.dirname(__file__)
    server = "Server%02d" % server_no
    file_path = os.path.join(base_dir, "dataset", date.day, '%s_%s%s.dat' % (server, date.to_date(), time.to_time()))
    return file_path


# def _create_user(cur, ca_card_number):
#     try:
#         sql = 'INSERT IGNORE INTO user(CACardNO) VALUES(\'%s\');' % ca_card_number
#         cur.execute(sql)
#     except Exception as e:
#         logging.error(e)
#
#
# def _create_id_and_seq(cur, event):
#     try:
#         _sql_template = 'INSERT INTO idandseq(MessageID,EventID,RandomSeq,CACardNO,Seq,Time,ServerTime,ServerNO) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'
#         t = event.enter_time if isinstance(event, ChannelEnterEvent) else ''
#         data_list = map(lambda x: "\'%s\'" % x, (
#             event.message_id, event.event_id, event.random_sequence, event.ca_card_number, event.sequence,
#             t,
#             event.server_time))
#         data_list = map(lambda b: u'null' if b == '\'\'' else b, data_list)
#         sql = _sql_template % tuple(data_list)
#         cur.execute(sql)
#         cur.execute('SELECT last_insert_id();')
#         return cur.fetchone()[0]
#     except Exception as e:
#         logging.error(e)
#
#
# def _create_service_and_program(cur, event, t_id):
#     try:
#         _sql_template = "INSERT INTO serviceandprogram (ServiceID,TSID,FreqPt,ChannelName,ProgramName,Authorization,SignalIntensity,SignalQuality,id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#         sql = _sql_template % tuple(map(lambda x: "\'%s\'" % x, (
#         event.service_id, event.ts_id, event.frequency, event.channel_name, event.program_name, event.license,
#         event.signal_intensity, event.signal_quality, t_id)))
#         cur.execute(sql)
#     except Exception as e:
#         logging.error(e)


def _create_channel_enter(cur, event):
    try:
        _sql_template = "INSERT INTO `channel_in_event`(" \
              "MessageID,EventID,RandomSeq,CACardNO,Seq,Time," \
              "ServiceID,TSID,FreqPt,ChannelName,ProgramName,Authorization,SignalIntensity,SignalQuality," \
              "SDVProgram,ServerTime,ServerNO" \
              ") VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        sql = _sql_template % tuple(map(lambda x: "\'%s\'" % x,
                                        (event.message_id, event.event_id, event.random_sequence, event.ca_card_number,
                                         event.sequence,event.enter_time,event.service_id,event.ts_id,event.frequency,
                                         event.channel_name,event.program_name,event.license,event.signal_intensity,
                                         event.signal_quality,event.is_sdv,event.server_time, event.server_no)))
        cur.execute(sql)
    except Exception as e:
        logging.error(e)


def _create_channel_exit(cur, event):
    try:
        _sql_template = "INSERT INTO `channel_exit_event`(" \
                        "MessageID,EventID,RandomSeq,CACardNO,Seq,EndTime,BeginTime," \
                        "ServiceID,TSID,FreqPt,ChannelName,ProgramName,Authorization,SignalIntensity,SignalQuality," \
                        "SDVProgram,Duration,ServerTime,ServerNO) " \
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        sql = _sql_template % tuple(map(lambda x: "\'%s\'" % x,
                                        (event.message_id, event.event_id, event.random_sequence, event.ca_card_number,
                                         event.sequence,event.end_time, event.start_time,event.service_id,event.ts_id,
                                         event.frequency, event.channel_name,event.program_name,event.license,
                                         event.signal_intensity,event.signal_quality,event.is_sdv, event.duration,
                                         event.server_time, event.server_no)))
        cur.execute(sql)
    except Exception as e:
        logging.error(e)


def _deal_file(path):
    with open(path, 'r') as fp:
        processing_pool = Pool(PROCESSING_NUMBER)

        processing_pool.map(_deal_line, fp.xreadlines())
        processing_pool.close()
        processing_pool.join()


def _deal_line(line):
    try:
        args_list = line.decode('gbk').strip().split('|')
        args_list.append(3)
        if args_list[1] == '21':
            _deal_enter_event(args_list)
        elif args_list[1] == '5':
            _deal_exit_event(args_list)
    except UnicodeDecodeError as e:
        logging.error(e)


def _deal_enter_event(args_list):
    con = pymysql.connect(host=HOST, port=PORT, user=USERNAME, passwd=PASSWORD, charset='utf8')
    cur = con.cursor()
    con.select_db(DB)
    try:
        event = ChannelEnterEvent(*tuple(args_list))
        # _create_user(cur, event.ca_card_number)
        # t_id = _create_id_and_seq(cur, event)
        # _create_service_and_program(cur, event, t_id)
        _create_channel_enter(cur, event)
        con.commit()
    except Exception as e:
        logging.error(e)
    finally:
        cur.close()
        con.close()


def _deal_exit_event(args_list):
    con = pymysql.connect(host=HOST, port=PORT, user=USERNAME, passwd=PASSWORD, charset='utf8')
    cur = con.cursor()
    con.select_db(DB)
    try:
        event = ChannelExitEvent(*tuple(args_list))
        #_create_user(cur, event.ca_card_number)
        #t_id = _create_id_and_seq(cur, event)
        #_create_service_and_program(cur, event, t_id)
        _create_channel_exit(cur, event)
        con.commit()
    except Exception as e:
        logging.error(e)
    finally:
        cur.close()
        con.close()


if __name__ == '__main__':
    ServerNo = 1
    start_time = clock()
    for nDay in range(12,15):
        for i in range(0,24):
            date = NTADate(nDay, 5, 2016)
            time = NTATime(i, 47, 22)
            path = make_in_path(ServerNo, date, time)
            _deal_file(path)

    cost_time = clock() - start_time
    info = 'Finished! cost time: %s' % cost_time
    logging.info(info)
