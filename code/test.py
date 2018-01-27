import re, random
import urllib.request

time_out = 3
count = 0
proxies = []
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/'
        '537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36'}

def get_proxy():
    try:
        req = urllib.request.Request('http://www.xicidaili.com/', None, headers)
    except:
        print('fail to get proxy!')
        return

    response = urllib.request.urlopen(req)
    html = response.read().decode('utf-8')

    print(html)

    rule = '<tr\sclass[^>]*>\s+' \
           '<td>.+</td>\s+' \
           '<td>(.*)?</td>\s+' \
           '<td>(.*)?</td>\s+' \
           '<td>(.*)?</td>\s+' \
           '<td>(.*)?</td>\s+' \
           '<td>(.*)?</td>\s+' \
           '<td>(.*)?</td>\s+' \
           '</tr>'
    regex = re.compile(rule, re.VERBOSE)

    res = re.search(regex, html)

    print(res)

    exit()
    p = re.compile(r'''<tr\sclass[^>]*>\s+
                        <td>.+</td>\s+
                        <td>(.*)?</td>\s+
                        <td>(.*)?</td>\s+
                        <td>(.*)?</td>\s+
                        <td>(.*)?</td>\s+
                        <td>(.*)?</td>\s+
                        <td>(.*)?</td>\s+
                        </tr>''', re.VERBOSE)

    proxy_list = p.findall(html)

    for each_proxy in proxy_list[1:]:
        if each_proxy[4] == 'HTTP':
            proxies.append(each_proxy[0] + ':' + each_proxy[1])

def change_proxy():
    # 随机从序列中取出一个元素
    proxy = random.choice(proxies)
    # 判断元素是否合理
    if proxy is None:
        proxy_support = urllib.request.ProxyHandler({})
    else:
        proxy_support = urllib.request.ProxyHandler({'http': proxy})
    opener = urllib.request.build_opener(proxy_support)
    opener.addheaders = [('User-Agent', headers['User-Agent'])]
    urllib.request.install_opener(opener)
    print('智能切换代理：%s' % ('本机' if proxy == None else proxy))

def get_req(url):
    # 先伪造一下头部吧,使用字典
    blog_eader = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.152 Safari/537.36',
        'Host': 'blog.csdn.net',
        'Referer': 'http://blog.csdn.net/',
        'GET': url
    }
    req = urllib.request.Request(url, headers=blog_eader)
    return req


if __name__ == '__main__':

    get_proxy()
    print('有效代理个数为 : %d' % len(proxies))