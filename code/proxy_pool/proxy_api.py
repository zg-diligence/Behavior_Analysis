"""
  Copyright(c) 2018 Gang Zhang
  All rights reserved.
  Author:Gang Zhang
  Date:2018.02.01
"""

from flask import Flask, jsonify, request
from log_handler import LogHandler
from proxy_manager import ProxyManager

app = Flask(__name__)

logger = LogHandler('proxy_api')

api_list = {
    'get': u'get an usable proxy',
    'get_all': u'get all proxy from proxy pool',
    'delete?proxy=127.0.0.1:8080': u'delete an unable proxy',
    'get_status': u'proxy statistics'
}


@app.route('/')
def index():
    return jsonify(api_list)


@app.route('/get/')
def get():
    proxy = ProxyManager(logger).get()
    return proxy if proxy else 'no proxy!'


@app.route('/get_all/')
def getAll():
    proxies = ProxyManager(logger).getAll()
    return jsonify(proxies)


@app.route('/delete/', methods=['GET'])
def delete():
    proxy = request.args.get('proxy')
    ProxyManager().delete(proxy)
    return 'success'


@app.route('/get_status/')
def getStatus():
    status = ProxyManager(logger).getNumber()
    return jsonify(status)


def run():
    app.run(host='0.0.0.0', port=5010)
