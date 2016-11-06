# endcoding = utf-8
'''
author : vex1023
email :  vex1023@qq.com
'''

import time
import pandas as pd
import requests
from requests.compat import json
from vxUtils.decorator import retry,threads

BASICHEADERS = {
    'Connection': 'Keep-Alive',
    'Accept': 'text/html, application/xhtml+xml, */*',
    'Accept-Language': 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko'
}


class WebAPIBaseError(Exception):
    '''
    WebAPI的异常类

    '''
    pass


class WebAPIBase():
    '''
     API 操作类
    通过这个类可以方便的通过 API 进行一系列操作
    '''

    def __init__(self):
        self.session = requests.session()
        return

    @threads(5)
    @retry(3)
    def request(self, method, url, params=None, data=None, headers=None, encoding='utf-8'):
        '''
        用于发起http请求,此处采用了异步多线程的方式,返回值实际为:Tomorrow对象.
        Parameters
        ----------
        method : http请求方法
        url : http请求网址
        params : http请求网址参数
        data : http请求body内填写的参数
        headers : http请求头中的参数
        encoding : 编码信息,默认为'utf-8'

        Returns
        -------
        返回请求的response对象.
        注意此处返回的实际结果为:Tomorrow对象,通过Tomorrow.xxx可以调用response的各类方法

        '''
        kwargs = {
            "params": params,
            "data": data,
            "headers": BASICHEADERS
        }

        if self.headers is not None and self.headers != {}:
            #    print(self.header,kwargs['headers'])
            kwargs['headers'].update(self.headers)
        if self.token is not None and self.token != {}:
            kwargs["params"].update(self.token)
        if data is not None and data != {}:
            kwargs["data"] = self.body_formater(kwargs['data'], encoding)

        if headers is not None and headers != {}:
            kwargs['headers'].update(headers)

        resp = requests.request(method=method, url=url, **kwargs)
        resp.raise_for_status()
        resp.encoding = encoding
        return resp

    def get(self, url, params={}, data={}, headers={}, encoding='utf-8'):
        '''
        使用GET方法获取信息
        Parameters
        ----------
        url : GET的网址
        params : GET网址的参数
        data : GET请求的body的数据.
        headers : GET的http headers信息
        encoding : 编码信息,默认为'utf-8'

        Returns
        -------
        返回请求的response对象.
        注意此处返回的实际结果为:Tomorrow对象,通过Tomorrow.xxx可以调用response的各类方法

        '''

        return self.request(
            method="get",
            url=url,
            params=params,
            data=data,
            headers=headers,
            encoding=encoding
        )

    def post(self, url, params={}, data={}, headers={}, encoding='utf-8'):
        '''
        使用POST方法获取信息
        Parameters
        ----------
        url : POST的网址
        params : POST网址的参数
        data : POST请求的body的数据.
        headers : POST的http headers信息
        encoding : 编码信息,默认为'utf-8'

        Returns
        -------
        返回请求的response对象.
        注意此处返回的实际结果为:Tomorrow对象,通过Tomorrow.xxx可以调用response的各类方法

        '''
        return self.request(
            method="post",
            url=url,
            params=params,
            data=data,
            headers=headers,
            encoding=encoding
        )

    @property
    def token(self):
        '''
        用于设定网址参数的令牌信息,或者是必须加带的时间戳信息
        Returns
        -------
        返回一个dict格式的字段如:{'token':'some token','timestamp':int(time.time())}

        '''
        return {}

    @property
    def headers(self):
        '''
        用于设定http header请求的参数信息
        Returns
        -------
        返回一个dict格式的字段如:{'apikeys':'some apikey'}

        '''
        return {}

    def body_formater(self, data, encoding='utf-8'):
        '''
        用于设置body的格式,以及编码格式的.
        Parameters
        ----------
        data : 需要传递的数据
        encoding : 编码信息,默认为'utf-8'

        Returns
        -------
        返回已经编码好的格式

        '''

        body = json.dumps(data, ensure_ascii=False)
        body = body.encode(encoding)
        return body


MAXLIMIT = 800


class DataMangerAPI(WebAPIBase):
    '''
    用于远程调用DataAPIServer的服务

    '''

    def __init__(self, userid, channelid, url_prefix):
        self._userid = userid
        self._channelid = channelid
        self._token = {}
        self._extire_time = 0
        self._url_prefix = url_prefix
        super(DataMangerAPI,self).__init__()
        return

    @property
    def token(self):
        '''
        创建token值。
        过期后，自动生成新的token
        '''
        t_now = int(time.time())

        # 如果超时了,重新更新token
        if t_now > self._extire_time:
            url = self._url_prefix + '/token'
            params = {'userid': self._userid, 'channelid': self._channelid, 'grant_type': 'create'}

            # 此处不可以使用self.get()函数,否则将导致锁死.
            resq = requests.request(method='GET', url=url, params=params)
            token_info = resq.json()

            if 'errorcode' in token_info.keys() and token_info['errorcode'] != 0:
                raise WebAPIBaseError(resq.text)
            self._token = token_info['token']
            self._extire_time = t_now + token_info['extrie_in']

        return {'token': self._token}

    def cal(self, start='', end=''):
        url = self._url_prefix + '/cal'
        params = {'start':start , 'end':end}
        resq = self.get(url=url,params=params)

        df = pd.DataFrame(resq.json())
        df = df.T
        return df

    def hq(self, stocklist):
        '''
        实时行情接口
        Parameters
        ----------
        stocklist : 上证和深证交易所的代码表

        Returns
        -------
        返回实时行情的dict格式

        '''
        if isinstance(stocklist, str):
            stocklist = [stocklist]
        urls = [self._url_prefix + '/hq?symbols=' + ','.join(stocklist[i:i + MAXLIMIT]) \
                for i in range(0, len(stocklist), MAXLIMIT)]
        resqs = [self.get(url) for url in urls]
        retval = dict()
        for resq in resqs:
            retval.update(resq.json())
        df = pd.DataFrame(retval)
        df = df.T
        return df

    def nav(self, ofundlist):
        '''

        Parameters
        ----------
        ofundlist : 基金代码列表

        Returns
        -------
        返回基金净值信息,以及实时估值.

        '''
        if isinstance(ofundlist, str):
            ofundlist = [ofundlist]
        urls = [self._url_prefix + '/nav?symbols=' + ','.join(ofundlist[i:i + MAXLIMIT]) \
                for i in range(0, len(ofundlist), MAXLIMIT)]

        resqs = [self.get(url) for url in urls]
        retval = dict()
        for resq in resqs:
            retval.update(resq.json())
        df = pd.DataFrame(retval)
        df = df.T
        #df.index.name='date'
        return df

    def bars(self, symbol, start='1900-01-01', end='', ktype=None):
        '''
        获取历史信息
        Parameters
        ----------
        symbollist : 证券代码列表
        start : 开始时间
        end : 结束时间,如果不指定,则为当前日期

        Returns
        -------
        返回股票开盘价,收盘价,最高价,最低价

        '''
        if start is None:
            start = '1900-01-01'
        if end is None:
            end = ''
        url = self._url_prefix + '/bars?start=%s&end=%s&symbol=%s' % (start, end, symbol)

        resqs = self.get(url)
        retval = resqs.json()
        if 'errorcode' in retval.keys() and retval['errorcode'] != 0:
            return retval

        df = pd.DataFrame(retval)
        #
        df = df.T
        #df.index = df.index.to_datetime()
        #df.index = df.index.tz_localize(pytz.utc)
        df.index.name='date'
        return df

    def update(self, methodlist=None):
        '''
        用于调用数据库更新函数
        Parameters
        ----------
        methodlist : 方法名称:['sinahq','hexunhq']

        Returns
        -------
        返回执行情况

        '''
        if isinstance(methodlist, str):
            methodlist = [methodlist]
        url = self._url_prefix + '/update?methodlist=' + ','.join(methodlist)
        resq = self.get(url)
        retval = resq.json()
        return retval

    @property
    def trade_status(self):
        '''
        检查当前交易所交易状态
        Returns
        -------
        {'errorcode': 0, 'status': 'close'/'trading'/'break', 'lastest_trade_day': '2016-02-05'}
        '''
        url = self._url_prefix + '/trade_status'
        resq = self.get(url)
        retval = resq.json()
        if 'errorcode' in retval.keys() and retval['errorcode'] != 0:
            raise WebAPIBaseError(retval)
        return retval
