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
table_nm = ['cust_acct_prod_info','cust_info']
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
            content = re.sub(r'bigint.*?(?=comment)|tinyint.*?(?=comment)|int.*?(?=comment)', "  bigint  ", lines)
            content = re.sub(r'varchar.*?(?=comment)|datetime.*?(?=comment)|char.*?(?=comment)|timestamp.*?(?=comment)', "  string ", content)
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

        for i in w:
            if len(i) > 1:
                col_nm=i.split()[0]
                col_type = i.split()[1]
                col_com = i.split()[2]
                col_comm = i.split()[3]
                str=col_nm.ljust(25)+col_type.ljust(20)+col_com.ljust(20)+col_comm.ljust(25)
                print(str)
                # w1.append(i)
        print "\n"
        # table_cols = []
        # for i in w1[1:-1]:
        #     table_cols.append(i.split()[0])
        # c = ','.join(table_cols) + ':' + table_cols[0]
        # col_nm[w1[0].strip('create table ods.ods')] = c
        # ods_table = w1[0].strip("create").strip("table").strip('  (;')
        # w95 = '\n'.join(w1) + ")"+'\n'+"partitioned by (dt string) row format delimited fields terminated by '\\t' lines terminated by '\\n';"
        # ods_95 = w95.replace(',)', ')')
        # print(ods_95 + '\n')


 # print "\n"
# print col_nm
# col = pd.Series(col_nm)
# print col
