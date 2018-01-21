# -*- coding:utf-8 -*-
'''
好像想错了，暂时没什么用了
'''
import os

class Node:
    def __init__(self):
        self.value = None
        self.children = {}    # children is of type {char, Node} or {1:xx,2:xx...}
        self.set = []


class Trie:
    def __init__(self):
        self.root = Node()

    def insert(self, key):
        node = self.root
        for char in key:
            if char not in node.children:
                child = Node()
                node.children[char] = child
                node = child
            else:
                node = node.children[char]
        node.value = key
        return node

    def search(self, key):
        node = self.root
        for char in key:
            if char not in node.children:
                return None
            else:
                node = node.children[char]
        return node

    def count(self, node, event):
        if node.value != None:
            if event not in node.children:
                node.children[event] = 1
            else:
                node.children[event] += 1
        return

    def count_line(self, node, line):
        if node.value != None:
            node.set.append(line)
        return

    def display_node(self, node, fp):
        if node.value != None:
           #print node.value,node.children
           fp.write('%s %s\n' % (node.value, node.children))
           return
        import string
        for char in '0123456789':
            if char in node.children:
                self.display_node(node.children[char],fp)
        return

    def display(self,fp):
        self.display_node(self.root,fp)

    def classify_node(self, node, day, hour):
        if node.value != None:
            #print node.value,node.children
            base_dir = os.path.dirname(__file__)
            outpath = os.path.join(base_dir, "CACardNO",str(day)+"_"+str(hour))
            if not os.path.exists(outpath):
                os.makedirs(outpath)
            outpath += '/'+node.value+".txt"
            #print outpath
            fp = open(outpath,'w')
            node.set.sort(key=lambda x:x.split('|')[-1])
            for i in node.set:
                fp.write(i)
            fp.close()
            return
        import string
        for char in '0123456789':
            if char in node.children:
                self.classify_node(node.children[char],day,hour)
        return

    def classify(self, day, hour):
        self.classify_node(self.root, day, hour)

'''
trie = Trie()
lines =[
'67|20|LtIBNutJocGyMhgHr|825010315307219|99616612220013801|20160501015931858\n',
'543|20|QMaDLlyfrrTIJDbiB|825010228253197|99586611270107556|20160501015931904\n',
'15729|20|QMaDLlyfrrTIJDbiB|825010251130418|02176615370018672|20160501015931905\n',
'708|20|imalcbJKfZPJUnvzk|825010237264360|99586611270101619|20160501015931931\n',
'604|20|PapbsQNqMyjPHrKop|825010219599761|02166615100083143|20160501015931952\n',
'251|20|MiEMkzMqJQRgUqLcp|825010385994961|02166615180105823|20160501015931994\n',
'67|20|LtIBNutJocGyMhgHr|825010315307219|99616612220013801|20160501015931852\n',
'67|21|LtIBNutJocGyMhgHr|825010315307219|99616612220013801|20160501015931851\n',
'67|20|LtIBNutJocGyMhgHa|825010315307219|99616612220013801|20160501015931851\n',
'67|20|LtIBNutJocGyMhgHa|825010315307219|99616612220013801|20160501015931852\n',
'67|20|LtIBNutJocGyMhgHr|825010315307219|99616612220013801|20160501015931855\n',
'67|21|LtIBNutJocGyMhgHr|825010315307219|99616612220013801|20160501015931853\n',
'67|20|LtIBNutJocGyMhgHr|825010315307219|99616612220013801|20160501015931854\n',
]
for tmp in lines:
    #print tmp
    key = tmp.split('|')[3]
    event = tmp.split('|')[1]
    find = trie.search(key)
    if find is None:
        find = trie.insert(key)
    #trie.count(find,event)
    trie.count_line(find,tmp)

#fp = open('try.txt','w')
#trie.display(fp)
#fp.close()
trie.classify(1, 2)
'''