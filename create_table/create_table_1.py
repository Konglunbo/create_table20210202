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
# 'cust_acct_prod_info','cust_info'
col_nm = defaultdict(list)
table_nm = ['lc_quota_audit_bj_attach_details']
for i in table_nm:
    sql = "show create table " + i
    df = pd.read_sql(sql, engine)
    content = df['Create Table'].values[0]
    # print content
    with open('C:\\Users\\kongfanxin.CITICCFC\\Desktop\\a.txt', 'w') as f:
        f.write(content)
    with open('C:\\Users\\kongfanxin.CITICCFC\\Desktop\\a.txt', 'r') as f:
        w = []
        for lines in f.readlines():
            lines = lines.lower()
            print(lines)
            content = re.sub(r'bigint.*?(?=comment) |tinyint.*?(?=comment)|int.*?(?=comment)', "  bigint  ", lines)
            content = re.sub(r'varchar.*?(?=comment)|datetime.*?(?=comment)|char.*?(?=comment)|timestamp.*?(?=comment) | longtext.*?(?=comment)', "  string ", content)
            decimal = re.findall(r'[(](.*?)[)]', content)
            if len(decimal)>0:content = re.sub(r'decimal.*?(?=comment)', "decimal("+decimal[0]+")   ", content)
            content = re.sub(r'`rs', "`ods.ods_rs", content)
            content = re.sub(r'` \(', " ` (", content)
            content = re.sub(r'primary.*$|unique.*$|key.*$|create table.*$|engine=innodb.*$', "", content)
            # content = content.replace('`', '')
            # content = content.replace(',', '')
            w.append(content.strip())
            # w.append(content)
        w1 = []

        print "---------建表语句-----------"
        for i in w:
            print i
            if len(i) > 1:
                splitStr=[]
                splitStr=i.split()
                if len(splitStr)>=4:
                    col_nm=splitStr[0]
                    col_type = splitStr[1]
                    col_com = splitStr[2]
                    col_comm = splitStr[3]
                    col_comm = col_comm.replace(',', '')
                    col_comm = col_comm.replace('\'', '')

                    str=','+col_nm.ljust(35)+col_type.ljust(20)+col_com.ljust(8)+'\''+col_comm+'\''
                elif len(splitStr)==3:
                    col_nm = splitStr[0]
                    col_type = splitStr[1]
                    str =','+ col_nm.ljust(35) + col_type.ljust(20)
                else:
                    col_nm=splitStr[0]
                    str=col_nm.ljust(25)
                print(str)


        print "---------Coalesce转换-----------"
        for i in w:
            if len(i) > 1:
                splitStr = []
                splitStr = i.split()
                if len(splitStr) >= 4:
                    col_nm = splitStr[0]
                    col_type = splitStr[1]
                    col_com = splitStr[2]
                    col_comm = splitStr[3]
                    col_comm = col_comm.replace(',', '')
                    col_comm = col_comm.replace('\'', '')
                    if col_type.startswith('decimal'):
                        str = ',COALESCE(T1.' + col_nm+', CAST(0 AS ' + col_type +')) -- ' + '\''+col_comm+'\''
                    else:
                        str =',T1.'+col_nm.ljust(25)+' -- '+ '\''+col_comm+'\''

                elif len(splitStr) == 3:
                    col_nm = splitStr[0]
                    col_type = splitStr[1]
                    if col_type.startswith('decimal'):
                        str = ',COALESCE(T1.' + col_nm + ', CAST(0 AS ' + col_type + ')) -- ' + '\''+col_comm+'\''
                    else:
                        str = ',T1.' + col_nm.ljust(25)
                else:
                    col_nm = splitStr[0]
                    str = col_nm.ljust(25)
                print(str)

        print "\n"
        print "---------码值转换----------"
        for i in w:
            if len(i) > 1:
                splitStr = []
                splitStr = i.split()
                if len(splitStr) >= 4:
                    col_nm = splitStr[0]
                    col_type = splitStr[1]
                    col_com = splitStr[2]
                    col_comm = splitStr[3]
                    col_comm = col_comm.replace(',', '')
                    col_comm = col_comm.replace('\'', '')
                    str = ',T1.' + col_nm.ljust(25) + ' -- ' + '\''+col_comm+'\''

                elif len(splitStr) == 3:
                    col_nm = splitStr[0]
                    col_type = splitStr[1]
                    str = ',T1.' + col_nm.ljust(25)
                else:
                    col_nm = splitStr[0]
                    str = col_nm.ljust(25)
                print(str)






