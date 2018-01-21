from bs4 import BeautifulSoup
from urllib.request import urlopen, quote

url = 'https://baike.baidu.com/item/' + quote('')
html = urlopen(url)
bsObj = BeautifulSoup(html, 'html.parser')
if bsObj.head.title.get_text() == '百度百科——全球最大中文百科全书':
    print('no page!')
else:
    data = bsObj.head.find_all('meta')
    print(data[3]['content'])
