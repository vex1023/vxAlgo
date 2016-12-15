# endcoding = utf-8
'''
author : 
email : 
'''

import os
import logging
import json
from datetime import timedelta, datetime
from random import randint
from multiprocessing.dummy import Queue
from multiprocessing.dummy import Process
from multiprocessing.queues import Empty

from functools import wraps
from vxAlgo import logger
from apscheduler.schedulers.blocking import BlockingScheduler as Scheduler


class Event():
    '''事件基础定义'''

    def __init__(self, event_type, event_data=None):
        self._type = event_type
        self._data = event_data

    @property
    def data(self):
        return self._data

    @property
    def type(self):
        return self._type

    def __repr__(self):
        return '< Event: type: %s, data: %s >' % (self.type, self.data)

    def __str__(self):
        return '< Event: type: %s, data: %s >' % (self.type, self.data)


class EventEngine():
    '''事件处理引擎'''

    def __init__(self):
        '''初始化'''

        # 创建队列
        self._queue = Queue()

        # 是否触发时间
        self._active = False

        # 线程池
        self._thread = [Process(target=self._run) for _ in range(5)]
        self._workers_n = 5

        # 执行
        self._handlers = {}

    def _run(self):

        logger.info('Worker启动...')
        while self._active:
            try:
                event = self._queue.get(block=True, timeout=0.3)
                logger.debug('准备处理时间: %s' % event)
                self._process(event)
            except Empty:
                pass

        logger.info('Worker停止...')
        return ''

    def _process(self, event):
        """处理事件"""
        # 检查是否存在对该事件进行监听的处理函数
        handlers = self._handlers.get(event.type, [])

        for handler in handlers:
            try:
                if callable(handler):
                    ret_events = handler(event)
                    if isinstance(ret_events, list) == False:
                        ret_events = [ret_events]

                    for ret_event in ret_events:
                        if isinstance(ret_event, Event):
                            self.trigger(ret_event)

            except Exception as err:
                logger.warning('事件: %s 运行错误: %s' % (event.type, err))

    def start(self):
        '''开始执行'''
        # 后台运行
        logger.info('启动后台线程')
        self._active = True

        for thread in self._thread:
            thread.start()

    def stop(self):
        logger.info('关闭后台线程')
        self._active = False

        # 等待后台线程结束
        for thread in self._thread:
            thread.join(timeout=1)

    def register(self, event_type, handler):
        '''注册监听事件'''
        logger.info('register event type: %s, handler: %s' % (event_type, handler))
        handlers = self._handlers.pop(event_type, [])
        if handler not in handlers:
            handlers.append(handler)
        self._handlers[event_type] = handlers

    def unregister(self, event_type, handler):
        '''取消注册监听事件'''
        logger.debug('unregister event type: %s, handler: %s' % (event_type, handler))
        handlers = self._handlers.pop(event_type, [])
        if handler in handlers:
            handlers.remove(handler)

        if handlers:
            self._handlers[event_type] = handlers

    def trigger(self, event):
        '''触发一个消息'''
        logger.debug('trigger event: %s' % event)
        self._queue.put(event)
        return

    def handle(self, event_type, context=None):
        def deco(func):
            @wraps(func)
            def event_handler(event):
                # TODO 检查参数，然后自动增加相应的数据进去
                return func(event=event, context=context)

            self.register(event_type, event_handler)

            return func

        return deco

    def on_tick(self, context=None):
        return self.handle('on_tick', context=context)

    def on_open(self, context=None):
        return self.handle('on_open', context=context)

    def before_trade(self, context=None):
        return self.handle('before_trade', context=context)

    def pre_close(self, context=None):
        return self.handle('pre_close', context=context)

    def on_close(self, context=None):
        return self.handle('on_close', context=context)

    def after_close(self, context=None):
        return self.handle('alfter_close', context=context)


class AlgoContext():
    def __init__(self, config_file='config.json', trader=None, logger=None, data=None):
        if not logger:
            logger = logging.getLogger('vxQuant.AlgoContext')

        self.logger = logger  # 日志
        self.trader = trader  # 交易接口
        self.data = data      # 数据接口
        self.config_file = config_file
        self._config = {}
        self.load()

    def load(self):

        self._config = {}
        if os.path.exists(self.config_file):

            with open(self.config_file, encoding='utf-8') as f:
                self._config = json.load(f)

    def save(self):
        with open(self.config_file, encoding='utf-8', mode='w') as f:
            f.write(json.dumps(self._config))

    def __getattr__(self, item):
        return self._config[item]

    def __setattr__(self, key, value):
        if key in ['_config', 'trader','logger', 'config_file', 'data']:
            self.__dict__[key] = value
        else:
            self._config[key] = value
            self.save()

    def __str__(self):
        tpl = '''< AlgoContext: trader: %s \n config: %s\n >'''
        return tpl %(str(trader), self._config)

    def __repr__(self):
        tpl = '''< AlgoContext: trader: %s \n config: %s\n >'''
        return tpl %(str(trader), self._config)


class AlgoTrade():
    def __init__(self, event_engine):
        self.event_engine = event_engine
        self._expire_at = datetime.now()
        self._trade_status = {'status':'close'}
        self.sched = Scheduler()

    def daily_jobs(self, data):

        logger.info('判断当前时候交易日')
        if data.market_status == 'close':
            logger.info('当前不是交易日，耐心等待')
            logger.info(self.sched.get_jobs())
            return

        logger.warning('当前为交易日，准备执行各类交易策略')
        on_open_event = Event('on_open', data)
        on_tick_event = Event('on_tick', data)
        before_trade_event = Event('before_trade', data)
        on_ipo_event = Event('on_ipo', data)
        pre_close_event = Event('pre_close', data)
        on_close_event = Event('on_close', data)

        self.sched.add_job(self.event_engine.trigger,'date',
                           run_date=data.market_am_open + timedelta(minutes=1),
                           args=[on_open_event],
                           id='on_open'
                           )

        self.sched.add_job(self.event_engine.trigger, 'date',
                           run_date=data.market_am_close + timedelta(minutes=randint(30, 90)),
                           args=[on_ipo_event],
                           id='on_ipo'
                           )

        self.sched.add_job(self.event_engine.trigger, 'date',
                           run_date=data.market_fm_close - timedelta(minutes=10),
                           args=[pre_close_event],
                           id='pre_close'
                           )

        self.sched.add_job(self.event_engine.trigger, 'date',
                           run_date=data.market_fm_close - timedelta(minutes=5),
                           args=[on_close_event],
                           id='on_close'
                           )

        self.sched.add_job(self.event_engine.trigger, 'interval', seconds=15,
                           start_date=data.market_am_open + timedelta(minutes=1),
                           end_date=data.market_am_close,
                           args=[on_tick_event],
                           id='on_tick_am'
                           )

        self.sched.add_job(self.event_engine.trigger, 'interval', seconds=15,
                           start_date=data.market_fm_open + timedelta(minutes=1),
                           end_date=data.market_fm_close - timedelta(minutes=5),
                           args=[on_tick_event],
                           id='on_tick_fm'
                           )

        self.event_engine.trigger(before_trade_event)
        logger.info(self.sched.get_jobs())

    def run(self, data):

        try:
            self.event_engine.start()
            self.sched.add_job(self.daily_jobs, 'cron', day_of_week='0-4', hour=9, minute=27, args=[data])
            self.daily_jobs(data)
            logger.warning('策略启动执行')
            self.sched.start()
        except:
            self.event_engine.stop()
            self.sched.shutdown(wait=True)
            logger.warning('策略停止执行成功')
