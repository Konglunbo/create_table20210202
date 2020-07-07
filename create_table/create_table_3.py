# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import re
from collections import defaultdict

import MySQLdb
import pandas as pd

engine = MySQLdb.connect(host='60.60.40.62', port=3306, user='liuyang', passwd='liuyang@2019', db='cmis_core',
                         connect_timeout=200, charset='utf8')

col_nm = defaultdict(list)
# 'cust_acct_prod_info','cust_info',
table_nm = ['lc_appl_limit_use']
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
            line = line.lower()
            # print(line)
            line = re.sub(r'`rs', "`ods.ods_rs", line)
            line = re.sub(r'` \(', " ` (", line)
            line = re.sub(r'primary.*$|unique.*$|key.*$|create table.*$|engine=innodb.*$', "", line)
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
                    col_type = re.sub(r'bigint.*|tinyint.*|int.*', "bigint  ", col_type)
                    col_type = re.sub(r'varchar.*|datetime.*|char.*|timestamp.*|longtext.*|date.*|text.*', "string ",col_type)
                    col_comm = splitStr[-1]
                    col_comm = col_comm.replace(',', '')
                    col_comm = col_comm.replace('\'', '')
                    str1=','+col_nm.ljust(35)+col_type.ljust(20)+'comment  \''+col_comm+'\''
                elif len(splitStr) == 4:
                    col_nm = splitStr[0]
                    col_type = splitStr[1]
                    col_type = re.sub(r'bigint.*|tinyint.*|int.*', "bigint  ", col_type)
                    col_type = re.sub(r'varchar.*|datetime.*|char.*|timestamp.*|longtext.*|date.*|text.*', "string ", col_type)
                    str1 = ',' + col_nm.ljust(35) + col_type.ljust(20) + 'comment  \'\' '
                else:
                    col_nm = splitStr[0]
                    str1 = col_nm.ljust(25)

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
                    col_type = re.sub(r'bigint.*|tinyint.*|int.*', "bigint  ", col_type)
                    col_type = re.sub(r'varchar.*|datetime.*|char.*|timestamp.*|longtext.*|date.*|text.*', "string ", col_type)
                    col_comm = splitStr[-1]
                    col_comm = col_comm.replace(',', '')
                    col_comm = col_comm.replace('\'', '')

                    if col_type.startswith('decimal'):
                        str2 = ',COALESCE(T1.' + col_nm+', CAST(0 AS ' + col_type +')) '.ljust(7) + '--  \''+col_comm+'\''
                    else:
                        str2 =',T1.'+col_nm.ljust(25).ljust(50)+ '--  \''+col_comm+'\''

                elif len(splitStr) == 4:
                    col_nm = splitStr[0]
                    col_type = splitStr[1]
                    col_type = re.sub(r'bigint.*|tinyint.*|int.*', "bigint  ", col_type)
                    col_type = re.sub(r'varchar.*|datetime.*|char.*|timestamp.*|longtext.*|date.*|text.*', "string ", col_type)
                    if col_type.startswith('decimal'):
                        str2 = ',COALESCE(T1.' + col_nm + ', CAST(0 AS ' + col_type + ')) -- ' + '\'' + col_comm + '\''
                    else:
                        str2 = ',T1.' + col_nm.ljust(25).ljust(50)+ '--  \'\''
                else:
                    col_nm = splitStr[0]
                    str2 = col_nm.ljust(25)
                print str2

        print "\n"
        print "---------码值转换----------"

        for i in w:
            if len(i) > 1:
                splitStr = []
                splitStr = i.split()
                if len(splitStr) >= 5:
                    col_nm = splitStr[0]
                    col_type = splitStr[1]
                    col_type = re.sub(r'bigint.*|tinyint.*|int.*', "bigint  ", col_type)
                    col_type = re.sub(r'varchar.*|datetime.*|char.*|timestamp.*|longtext.*|date.*|text.*', "string ", col_type)
                    col_comm = splitStr[-1]
                    col_comm = col_comm.replace(',', '')
                    col_comm = col_comm.replace('\'', '')

                    str3 = ',T1.' + col_nm.ljust(25) + ' -- ' + '\'' + col_comm + '\''

                elif len(splitStr) == 4:
                    col_nm = splitStr[0]
                    str3 = ',T1.' + col_nm.ljust(25) + ' -- ' + '\'\''
                else:
                    col_nm = splitStr[0]
                    str3 = col_nm.ljust(25)
                print str3





