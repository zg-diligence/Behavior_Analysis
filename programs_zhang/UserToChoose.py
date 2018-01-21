import os

a = set(os.listdir('CACardNo_Day/1'))

for i in range(1,11):
	b = set(os.listdir('CACardNo_Day/%d'%i))
	a = a & b
fp = open('UserToChoose.txt','w')
for c in a:
	fp.write('%s\n'%c)
fp.close()