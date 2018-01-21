# -*-coding:utf-8-*-
import os
from pygraphviz import *

# TargetUser = ['825010354067360','825010214580868','825010269689626','825010213844101','825010373957410','825010213836345','825010213880008','825010367831984',]
TargetUser = ['825010354067360','825010214580868','825010269689626','825010213844101','825010373957410','825010213836345','825010213880008','825010367831984',
			'825010278646143','825010213832088','825010385953672','825010213892153','825010278655112','825010278620036','825010226259155','825010226809731',
			'825010228233866','825010213801266','825010373952909','825010253081685','825010212925883','825010228331695','825010226233260','825010228330972',
			'825010257108005','825010226967702','825010269608391','825010226215141','825010228356599','825010213886096','825010226200454','825010226861145',
			'825010269685604','825010251163333',]
BASE_DIR = os.path.dirname(__file__)

for user in TargetUser:
	in_path = os.path.join(BASE_DIR, "Behavior_test", "map", user+'_map.txt')

	fp = open(in_path, 'r')
	nodes = list(eval(fp.readline()))
	edges = eval(fp.readline())
	fp.close()

	G = AGraph(strict=False, directed=True)
	# G.add_nodes_from(nodes)
	for temp_key,value in edges.items():
		if value<1000:
			continue
		if temp_key[0]!=temp_key[1]:
			G.add_edge(temp_key[0],temp_key[1],label=str(value))

	out_path = os.path.join(BASE_DIR, "Behavior_test", "map", user+'.png')
	G.layout(prog='dot')
	G.draw(out_path)


# in_path = os.path.join(BASE_DIR, "Behavior_test", "map", 'total_map.txt')

# fp = open(in_path, 'r')
# nodes = list(eval(fp.readline()))
# edges = eval(fp.readline())
# fp.close()

# for threshold in [0,100,200,300,500,1000,1500,2000,3000,4000,5000]:
# 	G = AGraph(strict=False, directed=True)
# 	# G.add_nodes_from(nodes)
# 	for temp_key,value in edges.items():
# 		if value<threshold:
# 			continue
# 		if temp_key[0]!=temp_key[1]:
# 			G.add_edge(temp_key[0],temp_key[1],label=str(value))

# 	out_path = os.path.join(BASE_DIR, "Behavior_test", "map", 'total_threshold_'+str(threshold)+'.png')
# 	G.layout(prog='dot')
# 	G.draw(out_path)