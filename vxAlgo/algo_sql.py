# endcoding = utf-8
'''
author :  vex1023
email :  vex1023@qq.com
'''

# 测试连通性
sql_ping = '''select 1 from dual;'''

# 用户查找 trader的信息
traders_info_sql = '''select * from trader_info t where t.status ='N';'''