# -*-coding:utf-8-*-
# 152.5s 164.2s
import pymysql
import os
from datetime import datetime,timedelta
import jieba

MainTargetEvents = ['21','5','6','13','17','23','97','7','96']
# TargetUser = ['825010354067360','825010214580868','825010269689626','825010213844101','825010373957410','825010213836345','825010213880008','825010367831984',]
# TargetUser = ['825010354067360','825010214580868','825010269689626','825010213844101','825010373957410','825010213836345','825010213880008','825010367831984','825010278646143','825010213832088','825010385953672','825010213892153','825010278655112','825010278620036','825010226259155','825010226809731',
# '825010228233866','825010213801266','825010373952909','825010253081685','825010212925883','825010228331695','825010226233260','825010228330972','825010257108005',
# '825010226967702','825010269608391','825010226215141','825010228356599','825010213886096','825010226200454','825010226861145','825010269685604','825010251163333',]

Threshold = timedelta(0,60)
# _
# 6-------->21------->5
# 		   |   _	|
# 		   |---6----|
# 		   |  |	|	|
# 		   |--7,8---|

# 	    __    __    __
# 24----26----97----26----6

# __    __    __
# 26----96----26


class ItemStruct():
	def __init__(self, line):
		item = line.split('|')
		self.messageID = int(item[0])
		self.eventID = item[1]
		self.randomSeq = item[2]
		self.CACardNo = item[3]
		self.Freq = item[4]
		self.ServerTime = item[-1]
		self.endTime = datetime.strptime(item[5], "%Y.%m.%d %H:%M:%S") if self.eventID!='20' else None # 除了20心跳事件没有时间外其他时间的事件位置固定
		if self.eventID in MainTargetEvents:
			if self.eventID == '5':#为了处理方便，把事件5的开始时间放到倒数第二位（ServerTime前），这样使得频道名都是tmp[9]
				StartTime = item.pop(6)
				item.insert(-1,StartTime)
				self.startTime = datetime.strptime(StartTime, "%Y.%m.%d %H:%M:%S")
			if self.eventID == '7':
				StartVol = item.pop(7)
				StartTime = item.pop(6)
				item.insert(-1,StartTime)
				item.insert(-1,StartVol)
				self.startTime = StartTime
			if self.eventID == '96':#【10->11】响应时间 【11->12】当前时间 【12->10】节目名
				programName = item.pop(12)
				item.insert(10,programName)


			self.channelName = item[9] if self.eventID!='96' else 'None'
			self.programName = item[10]
			if self.channelName == '':
				self.channelName='None'
		
			#self.item = item
# AutomataMap = {
# 	('0','6'):'6a',
# 	('6a','6'):'6a',
# 	('6a','24'):'21',
# 	('21','5'):'5',
# 	('21','6'):'6b',
# 	('6b','6'):'6b',
# 	('6b','5'):'5',
# 	('0','24'):'24',
# 	('24','26'):'26a',
# 	('26a','26'):'26a',
# 	('26a','97'):'97',
# 	('97','97'):'97',
# 	('97','26'):'26b',
# 	('26b','26'):'26b',
# 	('26b','6'):'6c',

# }


# class AutomataNode():
# 	def __init__(self):
# 		self.status = '0' # 0表示起始状态

# 	def __goto__(self, newEvent):


db = pymysql.connect(
    host='127.0.0.1',
    port=3306,
    user='root',
    password='12345678',
    db='classification',
    charset='utf8'
)

def findClass(channelName,keywords):
	sql1 = 'SELECT class FROM channel_classification WHERE channel=%s'
	sql2 = 'SELECT class FROM program_classification WHERE channel=%s and keyword=%s'
	sql3 = 'SELECT class FROM program_classification WHERE keyword=%s'
	with db.cursor() as cursor:
		cursor.execute(sql1, (channelName))
		result = cursor.fetchone()
		result = result[0] if result else ''
		if result != '':
			return [(result, 100)]
		else:
			classes = {}
			classes2 = {}
			for keyword in keywords:
				cursor.execute(sql2,(channelName,keyword))
				result = cursor.fetchone()
				result = result[0] if result else ''
				if result != '':
					if result in classes:
						classes[result] += 1
					else:
						classes[result] = 1

				cursor.execute(sql3,(keyword))
				results = cursor.fetchall()
				for result in results:
					result = result[0] if result else ''
					if result != '':
						if result in classes2:
							classes2[result] += 1
						else:
							classes2[result] = 1
			if classes:
				total = sum(classes.values())
				res = []
				for key,value in classes.items():
					res.append((key,value*100.0/total)) 
				# res = sorted(classes.items(), key=lambda x:x[1])
				return res
			elif classes2:
				total = sum(classes2.values())
				res = []
				for key,value in classes2.items():
					res.append((key,value*100.0/total)) 
				# res = sorted(classes.items(), key=lambda x:x[1])
				return res
			else:
				return [(u'其他', 100)]
	db.commit()

BASE_DIR = os.path.dirname(__file__)

def string_cut(str):
	return str.strip()[:-4]

fp = open('UserToChoose.txt','r')
TargetUser = map(string_cut,fp.readlines())
TargetUser = TargetUser[:200]
fp.close()

res_fp = open('result_num_day.txt','a')
res_fp.write('num_day\trunning_time\n')

for num_day in range(1,11):
	start = datetime.now()
	############
	for user in TargetUser:
		# print 'Working with user:',user
		items_group = []
		cycle_items = []#randomSeq相同的一个cycle
		ClassStat = {}
		# print '  Reading data...'
		for i in range(1,num_day+1):
			in_path = os.path.join(BASE_DIR, "CACardNO_Day", "%d" % (i), user+'.txt')
			try:
				fp = open(in_path, 'r')
			except IOError:
				print 'No such path:',in_path
				continue
			for line in fp.xreadlines():
				item = ItemStruct(line.decode('gbk'))
				if cycle_items and cycle_items[-1].randomSeq != item.randomSeq:
					items_group.append(cycle_items)
					cycle_items = []
				cycle_items.append(item)
		items_group.append(cycle_items)
		fp.close()

		behaviors = ['Look Through', 'Time-shifted Play', 'VOD Play', 'Other']
		stack = []

		out_path = os.path.join(BASE_DIR, "Behavior_test", user+'_withThreshold1Min.txt')

		fp = open(out_path, 'w')
		for cycle_items in items_group:
			# print '  Find behavior with a new cycle...'
			for item in cycle_items:
				if item.eventID == '21':
					stack.append(item)
				elif item.eventID == '5':
					while stack and (stack[-1].eventID!='21' \
					or stack[-1].channelName!=item.channelName \
					or stack[-1].programName!=item.programName):
						stack.pop()
					# if stack:
					if stack and item.endTime-item.startTime>Threshold:
						keywords = [word for word in jieba.cut(item.programName) if len(word)>=2 and word!='NULL' and word!=u'以播出为准']
						programClasses = findClass(item.channelName, keywords)
						words = ','.join(keywords)
						# print '    Find Behavior!'
						fp.write('----------Find Behavior----------\n')
						fp.write(('Behavior:'+behaviors[0]+'\n').encode('utf8'))
						fp.write(('Start Time:'+str(item.startTime)+'\n').encode('utf8'))
						fp.write(('End Time:'+str(item.endTime)+'\n').encode('utf8'))
						fp.write(('Duration:'+str(item.endTime-item.startTime)+'\n').encode('utf8'))
						fp.write(('Channel Name:'+item.channelName+'\n').encode('utf8'))
						fp.write(('Program Name:'+item.programName+'\n').encode('utf8'))
						fp.write(('Keywords:'+words+'\n').encode('utf8'))
						fp.write('Possible Class(es) and Possibility:\n')
						for cla_poss in programClasses:
							# print '**************',cla_poss
							# fp.write(('\t'+cla_poss[0]+'\t'+str(cla_poss[1])+'%\n').encode('utf8'))
							fp.write(("\t%s\t%.2f%%\n"%(cla_poss[0],cla_poss[1])).encode('utf8'))
							if cla_poss[0] in ClassStat:
								ClassStat[cla_poss[0]] += item.endTime-item.startTime
							else:
								ClassStat[cla_poss[0]] = item.endTime-item.startTime
						fp.write('--------------End---------------\n\n\n\n\n')
						stack.pop()
				elif item.eventID == '26':
					if stack and (stack[-1].eventID=='97' or stack[-1].eventID=='96'):
						# for i in stack:
						# 	print i.eventID
						# endTime = stack.pop().endTime
						endTime = item.endTime
						pt = len(stack)-1
						while pt>=0 and (stack[pt].eventID=='97' or stack[pt].eventID=='96'):
							pt -= 1
						t_item = stack[pt+1]
						startTime = t_item.endTime
						duration = endTime - startTime
						channelName = t_item.channelName
						programName = t_item.programName
						behavior = behaviors[1] if t_item.eventID=='97' else behaviors[2]
						stack = stack[:pt+1]
						while stack and (stack[-1].eventID=='26' or stack[-1].eventID=='24'):
							stack.pop()

						if stack and duration>Threshold:
							keywords = [word for word in jieba.cut(programName) if len(word)>=2 and word!='NULL' and word!=u'以播出为准']
							programClasses = findClass(channelName, keywords)
							words = ','.join(keywords)
							# print '    Find Behavior!'
							fp.write('----------Find Behavior----------\n')
							fp.write(('Behavior:'+behavior+'\n').encode('utf8'))
							fp.write(('Start Time:'+str(startTime)+'\n').encode('utf8'))
							fp.write(('End Time:'+str(endTime)+'\n').encode('utf8'))
							fp.write(('Duration:'+str(duration)+'\n').encode('utf8'))
							fp.write(('Channel Name:'+channelName+'\n').encode('utf8'))
							fp.write(('Program Name:'+programName+'\n').encode('utf8'))
							fp.write(('Keywords:'+words+'\n').encode('utf8'))
							fp.write('Possible Class(es) and Possibility:\n')
							for cla_poss in programClasses:
								# fp.write(('\t'+cla_poss[0]+'\t'+str(cla_poss[1])+'%\n').encode('utf8'))
								fp.write(("\t%s\t%.2f%%\n"%(cla_poss[0],cla_poss[1])).encode('utf8'))
								if cla_poss[0] in ClassStat:
									ClassStat[cla_poss[0]] += duration
								else:
									ClassStat[cla_poss[0]] = duration
							fp.write('--------------End---------------\n\n\n\n\n')
						if stack:
							stack.pop()
					else:
						stack.append(item)

				elif item.eventID == '97' or item.eventID == '96':
					stack.append(item)

				elif item.eventID == '24':
					stack = [item]

				elif item.eventID == '6':
					if stack and stack[-1].eventID=='26':
						stack = [item]
					else:
						stack.append(item)
		fp.close()

		# out_path = os.path.join(BASE_DIR, "Behavior_test", user+'_Top5Class.txt')
		out_path = os.path.join(BASE_DIR, "Behavior_test", user+'_Top5_or_over50%_Class.txt')
		fp = open(out_path,'w')
		total = sum(map(timedelta.total_seconds,ClassStat.values()))
		TOP5 = sorted(ClassStat.items(), key=lambda x:x[1], reverse=True)#[:5]
		# fp.write("TOP 5 Classes of the user's favorite:\n")
		fp.write("TOP Classes of the user's favorite:\n")
		total_percent = 0
		cnt = 0
		for item in TOP5:
			fp.write(("%s %.2f%%\n"%(item[0],item[1].total_seconds()*100.0/total)).encode('utf8'))
			total_percent += item[1].total_seconds()*100.0/total
			cnt += 1
			if total_percent>=50 or cnt>=5:
				break
		fp.close()
		# fp = open('classify_test/')
	####
	end = datetime.now()
	res_fp.write('%d\t%s\n'%(num_day,str(end-start)))

res_fp.close()