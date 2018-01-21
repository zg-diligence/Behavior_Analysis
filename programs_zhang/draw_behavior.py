# -*-coding:utf-8-*-

import pymysql
import os
from datetime import datetime,timedelta
import jieba

MainTargetEvents = ['21','5','6','13','17','23','97','7','96']
# TargetUser = ['825010354067360','825010214580868','825010269689626','825010213844101','825010373957410','825010213836345','825010213880008','825010367831984',]
TargetUser = ['825010354067360','825010214580868','825010269689626','825010213844101','825010373957410','825010213836345','825010213880008','825010367831984','825010278646143','825010213832088','825010385953672','825010213892153','825010278655112','825010278620036','825010226259155','825010226809731',
'825010228233866','825010213801266','825010373952909','825010253081685','825010212925883','825010228331695','825010226233260','825010228330972','825010257108005',
'825010226967702','825010269608391','825010226215141','825010228356599','825010213886096','825010226200454','825010226861145','825010269685604','825010251163333',]

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
		

BASE_DIR = os.path.dirname(__file__)

items_group = []#for total
cycle_items = []#for total
for user in TargetUser:
	print 'Working with user:',user
	# items_group = []#for individual
	# cycle_items = []#randomSeq相同的一个cycle
	ClassStat = {}
	print '  Reading data...'
	for i in range(2,32):
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

# out_path = os.path.join(BASE_DIR, "Behavior_test", "map", 'total_map.txt')

# nodes = set()
# edges = {}

# for cycle_items in items_group:
# 	print '  Dealing with event in a new cycle...'
# 	pre_item = None
# 	for item in cycle_items:
# 		nodes.add(item.eventID)
# 		if pre_item and item.messageID==pre_item.messageID+1:
# 			if (pre_item.eventID,item.eventID) in edges:
# 				edges[(pre_item.eventID,item.eventID)] += 1
# 			else:
# 				edges[(pre_item.eventID,item.eventID)] = 1
# 		pre_item = item
# fp = open(out_path, 'w')
# fp.write(str(nodes))
# fp.write('\n')
# fp.write(str(edges))
# fp.close()

	out_path = os.path.join(BASE_DIR, "Behavior_test", "map", user+'_map.txt')

	nodes = set()
	edges = {}
	
	for cycle_items in items_group:
		print '  Dealing with event in a new cycle...'
		pre_item = None
		for item in cycle_items:
			nodes.add(item.eventID)
			if pre_item and item.messageID==pre_item.messageID+1:
				if (pre_item.eventID,item.eventID) in edges:
					edges[(pre_item.eventID,item.eventID)] += 1
				else:
					edges[(pre_item.eventID,item.eventID)] = 1
			pre_item = item
	fp = open(out_path, 'w')
	fp.write(str(nodes))
	fp.write('\n')
	fp.write(str(edges))
	fp.close()
