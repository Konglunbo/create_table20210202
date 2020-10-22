# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import re
from collections import defaultdict

import MySQLdb
import pandas as pd

engine = MySQLdb.connect(host='10.18.1.20', port=3306, user='liuyang', passwd='liuyang@2019', db='cmis_hs',
                         connect_timeout=200, charset='utf8')

col_nm = defaultdict(list)
# 'cust_acct_prod_info','cust_info',
table_nm = ['lm_channel_setlmt_log']
for i in table_nm:
    sql = "show create table " + i
    df = pd.read_sql(sql, engine)
    content = df['Create Table'].values[0]
    # print content
    with open('C:\\Users\\kongfanxin.CITICCFC\\Desktop\\a.txt', 'w') as f:
        f.write(content)
    with open('C:\\Users\\kongfanxin.CITICCFC\\Desktop\\a.txt', 'r') as f:
        w = []
        for line in f.readlines():
            line = line.upper()
            # print(line)
            line = re.sub(r'`RS', "`ODS.ODS_RS", line)
            line = re.sub(r'` \(', " ` (", line)
            line = re.sub(r'`', "", line)
            line = re.sub(r'PRIMARY.*$|UNIQUE.*$|KEY.*$|CREATE TABLE.*$|ENGINE=INNODB.*$', "", line)
            w.append(line.strip())

        print "\n"
        print "---------建表语句-----------"
        for i in w:
            if len(i) > 1:
                splitStr = []
                splitStr = i.split()
                if len(splitStr)>=5:
                    col_nm=splitStr[0]
                    col_type = splitStr[1]
                    col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT  ", col_type)
                    col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*', "STRING ",col_type)
                    col_comm = splitStr[-1]
                    col_comm = col_comm.replace(',', '')
                    col_comm = col_comm.replace('\'', '')
                    str1='"'+col_nm+'",'
                elif len(splitStr) == 4:
                    col_nm = splitStr[0]
                    col_type = splitStr[1]
                    col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT  ", col_type)
                    col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*', "STRING ", col_type)
                    str1='"'+col_nm+'",'
                else:
                    col_nm = splitStr[0]
                    str1='"'+col_nm+'",'

                print str1

        print "\n"
        print "---------Coalesce转换-----------"


        for i in w:
            if len(i) > 1:
                splitStr = []
                splitStr = i.split()
                if len(splitStr) >= 5:
                    col_nm = splitStr[0]
                    col_type = splitStr[1]
                    col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT", col_type)
                    col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*', "STRING", col_type)
                    col_comm = splitStr[-1]
                    col_comm = col_comm.replace(',', '')
                    col_comm = col_comm.replace('\'', '')

                    col_type1=col_type+')'
                    str1 = '{"name":"' + col_nm + '","type":"'+col_type+'"},'
                    print str1


                elif len(splitStr) == 4:
                    col_nm = splitStr[0]
                    col_type = splitStr[1]
                    col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT", col_type)
                    col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*', "STRING", col_type)
                    col_type1=col_type+')'
                    str1 = '{"name":"' + col_nm + '","type":"'+col_type+'"},'
                    print str1
                else:
                    col_nm = splitStr[0]
                    str2 = col_nm.ljust(25)








