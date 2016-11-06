# endcoding = utf-8
'''
author :  vex1023
email : vex1023@qq.com
'''

import requests
import records
import logging
import json
from apscheduler.events import EVENT_JOB_ERROR
from apscheduler.schedulers.blocking import BlockingScheduler as Scheduler
from vxUtils.PrettyLogger import add_console_logger, add_file_logger
from datetime import datetime, timedelta
from vxTrader import create_trader
from vxAlgo.algo_sql import traders_info_sql, sql_ping

_SINA_STOCK_KEYS = [
    "name", "open", "yclose", "lasttrade", "high", "low", "bid", "ask",
    "volume", "amount", "bid1_m", "bid1_p", "bid2_m", "bid2_p", "bid3_m",
    "bid3_p", "bid4_m", "bid4_p", "bid5_m", "bid5_p", "ask1_m", "ask1_p",
    "ask2_m", "ask2_p", "ask3_m", "ask3_p", "ask4_m", "ask4_p", "ask5_m",
    "ask5_p", "date", "time", "status"]

# 定义现金及现金等价物
_CASH = ['cash', 'sh511010', 'sh511880'] # 现金， 国债ETF， 银华日利

logger = logging.getLogger('AlgoRunner')
add_console_logger(logger,level='debug')
add_file_logger('AlgoRunner', 'debug', '/tmp/algo.log')


class AlgoError(Exception):
    pass


class Strategy():
    def __init__(self, trader, **kwargs):
        #self.name = name
        self.trader = trader
        pass

    def pre_start(self, data):
        '''每个交易日9点15分执行'''
        pass

    def on_start(self, data):
        '''每个交易日9点31人执行'''
        pass

    def on_tick(self, data):
        '''每个交易日交易时段：9:31-11:30 and 13:00-14:50 每个10秒执行一次'''
        pass

    def on_close(self, data):
        '''每个交易日14:55分执行，准备收盘时'''
        pass

    def pre_close(self, data):
        '''每个交易日14:50分执行'''
        pass

    def after_close(self, data):
        '''每个交易日记录收盘数据'''
        pass

    def cashout(self, volume):
        portfolio = self.trader.portfolio
        cash_value = portfolio.loc[portfolio.index.isin(_CASH),'market_value'].sum()
        if cash_value < volume:
            raise ValueError('not enought money')

        # 先检查一下当前现金是否足够，如果足够则返回，0
        if portfolio.loc['cash', 'market_value'] > volume:
            return True
        else:
            remain = volume - portfolio.loc['cash', 'market_value']

        for cash_symbol in _CASH[1:]:
            if cash_symbol in portfolio.index:
                if portfolio.loc[cash_symbol, 'market_value'] > remain:
                    order_volume = remain
                else:
                    order_volume = portfolio.loc[cash_symbol, 'market_value']
                self.trader.order(cash_symbol, -order_volume)
                portfolio = self.trader.portfolio
                # 先检查一下当前现金是否足够，如果足够则返回，0
                if portfolio.loc['cash', 'market_value'] > volume:
                    return 0
                else:
                    remain = volume - portfolio.loc['cash', 'market_value']

        return remain




class AlgoRunner():
    def __init__(self, enginestr,traders, logger=None, debug=False):

        # 初始化日志功能
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('AlgoRunner')
            add_console_logger(self.logger, level='info')
        # 开启debug功能
        if debug:
            self.logger.setLevel('debug'.upper())

        # 初始化日志
        self.db = ''
        #self.db = records.Database(enginestr)
        # 测试一下连通性
        #self.db.query(sql_ping)

        # 初始化策略仓库
        self.traders = traders
        self._strategys = []

        # 记录市场状态
        self._trade_status = ''
        self._expire_at = datetime.now()

        # 增加scheduler
        self.sched = Scheduler()

    def load_traders(self):
        '''加载traders'''
        # 加载traders，如果没有设定traders，报错
        traders = {}
        try:
            traders_info = self.db.query(traders_info_sql)
        except Exception as err:
            self.logger.warning('db query err: %s' % err)
            time.sleep(0.1)
            traders_info = self.db.query(traders_info_sql)

        for row in traders_info:
            try:
                kwargs = json.loads(row.kwargs)
            except Exception as err:
                self.logger.warning('kwargs parser error. kwargs: %s. Exception: %s' % (row.kwargs,err))
                kwargs = {}
            self.logger.info('Loading trader: %s' % row.traderid)
            traders[row.traderid] = create_trader(row.brokerid, row.account, row.password, **kwargs)

        self.logger.info('Loaded traders: %s' % traders.keys())
        return traders


    @property
    def trade_status(self):
        '''当前市场交易情况:
        { 'status': trading| break| close,
          'time_point': { 'am_open': datetime(x,x,x,x,x,x),
                          'am_close': datetime(x,x,x,x,x,x),
                          'fm_open': datetime(x,x,x,x,x,x),
                          'fm_close' : datetime(x,x,x,x,x,x)
                        }
        }
        '''
        now = datetime.now()
        if now < self._expire_at:
            return self._trade_status

        url = 'http://hq.sinajs.cn/?list=sh000001'
        r = requests.get(url)
        r.raise_for_status()
        line = r.text.splitlines()[0]
        hq = dict(zip(_SINA_STOCK_KEYS, line.split('"')[1].split(',')))

        hq_date = datetime.strptime(hq['date'] + ' ' + hq['time'], '%Y-%m-%d %H:%M:%S')
        self.logger.debug('行情时间: %s' % hq_date.strftime('%Y-%m-%d %H:%M:%S'))

        time_point = {
            'am_open': now.replace(hour=9, minute=25, second=0, microsecond=0),
            'am_close': now.replace(hour=11, minute=30, second=0, microsecond=0),
            'fm_open': now.replace(hour=13, minute=0, second=0, microsecond=0),
            'fm_close': now.replace(hour=15, minute=0, second=0, microsecond=0)
        }



        if hq_date < time_point['am_open']:
            self._trade_status = {
                'status': 'close',
                'time_point': {}
            }
            if now < time_point['am_open']:
                self._expire_at =time_point['am_open']
            else:
                self._expire_at = time_point['am_open'] + timedelta(days=1)
        elif now < time_point['am_close']:
            self._trade_status = {
                'status': 'trading',
                'time_point': time_point
            }
            self._expire_at = time_point['am_close']
        elif now < time_point['fm_open']:
            self._trade_status = {
                'status': 'break',
                'time_point': time_point
            }
            self._expire_at = time_point['fm_open']
        elif now < time_point['fm_close']:
            self._trade_status = {
                'status': 'trading',
                'time_point': time_point
            }
            self._expire_at = time_point['fm_close']
        else:
            self._trade_status = {
                'status': 'close',
                'time_point': time_point
            }
            self._expire_at = time_point['am_open'] + timedelta(days=1)

        return self._trade_status


    def add_strategy(self, strategy):
        '''导入策略'''
        self._strategys.append(strategy)

    def run(self, datasource):
        '''运行algorunner'''

        # 如果是在日间进行启动的，开始执行相应的任务
        self.logger.info('执行交易日准备工作')
        self._prepare(datasource)
        # 每天早上检查一下是否交易日的工作任务
        self.logger.info('每日交易日准备工作加入定时任务: self._prepare')
        self.sched.add_job(self._prepare, 'cron', day_of_week='0-4', hour=9, minute=20, args=[datasource])

        try:
            self.sched.start()
        except Exception as err:
            self.logger.warning('AlgoRunner退出: %s' % err)
            self.sched.shutdown(wait=True)
            # TODO 保存一下相关的参数

    def _prepare(self, datasource):
        '''交易日的各类批量任务'''

        # 检查现在是否是交易时间，如果不是，则直接返回
        self.logger.info('检查当前是否为交易时段: %s' % self.trade_status)
        if self.trade_status['status'] == 'close':
            self.logger.warning('现在不是交易时间。\n下次检查时间: %s' % self._expire_at.strftime('%Y-%m-%d %H:%M:%S'))
            return

        self.logger.warning('当前为交易时段，开始准备执行当天的交易任务')
        # 增加trader的keepliave 任务
        #traders = self.load_traders()
        for trader in self.traders.values():
            self.sched.add_job(trader.keepalive, 'interval',seconds=120,
                           start_date=self.trade_status['time_point']['am_open'],
                           end_date=self.trade_status['time_point']['fm_close']+timedelta(minutes=10))
            # TODO 增加每天将持仓保存的任务
            # 待续

        self.logger.info('增加trader keepalive任务')

        for s in self._strategys:
            # 增加准备工作
            self.sched.add_job(s.pre_start, 'date',
                               run_date=self.trade_status['time_point']['am_open']+timedelta(minutes=5),
                               args=[datasource])

            # 增加开市时点检查工作
            self.sched.add_job(s.on_start, 'date',
                               run_date=self.trade_status['time_point']['am_open'] + timedelta(minutes=10),
                               args=[datasource])

            # 增加日内行情监控
            self.sched.add_job(s.on_tick,'interval', seconds=20,
                               start_date = self.trade_status['time_point']['am_open'],
                               end_date = self.trade_status['time_point']['fm_close'] - timedelta(minutes=5),
                               args=[datasource])

            # 增加临时收盘操作提前10分钟
            self.sched.add_job(s.pre_close, 'date',
                               run_date=self.trade_status['time_point']['fm_close'] - timedelta(minutes=10),
                               args=[datasource])

            # 增加收盘时点操作
            self.sched.add_job(s.on_close, 'date',
                               run_date=self.trade_status['time_point']['fm_close'] - timedelta(minutes=5),
                               args=[datasource])

            # 增加收盘以后的批处理操作
            self.sched.add_job(s.after_close, 'date',
                               run_date=self.trade_status['time_point']['fm_close']+ timedelta(minutes=10),
                               args=[datasource])
