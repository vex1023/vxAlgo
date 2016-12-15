# encoding = utf-8
'''
这是一个python项目包的样例
'''

__version__ = '0.0.1'
__author__ = 'vex1023'
__email__ = 'vex1023@qq.com'

import logging
from vxUtils.PrettyLogger import add_console_logger

logger = logging.getLogger('vxQuant.vxAlgo')
add_console_logger(logger)



