import codecs, re

files = ['files/all_0.txt', 'files/all_1.txt', 'files/all_2.txt']
TV_palys, episode, others= [], [], []

for path in files:
    with codecs.open(path, 'r', encoding='utf8') as fr:
        for line in fr:
            item = line.strip()
            if re.search('剧场', item, re.M|re.I) and re.search('\(*[0-9]+\)*$', item, re.M|re.I):
                TV_palys.append(item)
            elif re.search('电视剧', item, re.M|re.I):
                TV_palys.append(item)
            elif re.search('第[0-9]+集$', item, re.M|re.I):
                episode.append(item)
            else:
                others.append(line.strip())

with codecs.open('TV_plays.txt', 'w', encoding='utf8') as fw:
    fw.write('\n'.join(TV_palys))

with codecs.open('TV_episode.txt', 'w', encoding='utf8') as fw:
    fw.write('\n'.join(episode))

with codecs.open('TV_others.txt', 'w', encoding='utf8') as fw:
    fw.write('\n'.join(others))