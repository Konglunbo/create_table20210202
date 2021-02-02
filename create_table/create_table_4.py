# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import re
from collections import defaultdict

import MySQLdb
import pandas as pd

engine = MySQLdb.connect(host='10.18.1.20', port=3306, user='liuyang', passwd='liuyang@2019', db='CMIS_CORE',
                         connect_timeout=200, charset='utf8')

col_nm = defaultdict(list)
# 'cust_acct_prod_info','cust_info',
table_nm = ['LC_APPL']
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
            line = re.sub(r'PRIMARY.*$|UNIQUE.*$|KEY.*$|CREATE TABLE.*$|ENGINE=INNODB.*$', "", line)
            w.append(line.strip())

        print "\n"
        print "---------建表语句-----------"
        for i in w:
            if len(i) > 1:
                splitStr = []
                splitStr = i.split()
                col_nm = splitStr[0]
                col_type = splitStr[1]
                col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT  ", col_type)
                col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*',"STRING ", col_type)
                col_comm = re.findall("'([^']*)'", i)
                col_comm= ''.join(col_comm)
                str1 = ",%-20s  %-20s COMMENT  '%-1s'" % (col_nm, col_type, col_comm)


                print str1

        print "\n"
        print "---------Coalesce转换-----------"


        for i in w:
            if len(i) > 1:
                splitStr = []
                splitStr = i.split()
                col_nm = splitStr[0]
                col_type = splitStr[1]
                col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT  ", col_type)
                col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*', "STRING ",col_type)
                col_comm = re.findall("'([^']*)'", i)
                col_comm= ''.join(col_comm)
                col_type1 = col_type + ')'
                print (",CAST(T1.%-30s  AS  %-30s    AS %-30s --  %-20s" % (col_nm, col_type1, col_nm, col_comm))


        print "\n"
        print "---------码值转换----------"

        for i in w:
            if len(i) > 1:
                splitStr = []
                splitStr = i.split()
                col_nm = splitStr[0]
                col_type = splitStr[1]
                col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT  ", col_type)
                col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*', "STRING ",
                                  col_type)
                col_comm = re.findall("'([^']*)'", i)
                col_comm= ''.join(col_comm)
                print (",T1.%-30s  AS  %-30s -- %-20s" % (col_nm, col_nm, col_comm))






