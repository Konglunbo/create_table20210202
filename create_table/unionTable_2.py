# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import re
from collections import defaultdict

import MySQLdb
import pandas as pd

# 数据样例
# APPL_NO APPL_SEQ
# APPL_DT APPLY_DT
# APPL_TIME   APPLY_DT_TIME
# APPL_AMT    APPLY_AMT
# APPRV_AMT   APPRV_AMT
# LOAN_PURPOSE    PURPOSE
# LOAN_TYPE   TYP_GRP
# ASSET_TAG   NULL
# PROD_ID LOAN_TYP
# CUST_ID CUST_ID

with open('C:\\SCRT\\a.txt', 'r') as f:
    w = []
    for line in f.readlines():
        line = line.upper()
        # print(line)
        line = re.sub(r'`RS', "`ODS.ODS_RS", line)
        line = re.sub(r'` \(', " ` (", line)
        line = re.sub(r'PRIMARY.*$|UNIQUE.*$|KEY.*$|CREATE TABLE.*$|ENGINE=INNODB.*$', "", line)
        w.append(line.strip())






    print "---------建表语句-----------"
    for i in w:
        if len(i) > 1:
            splitStr = []
            splitStr = i.split()
            if len(splitStr)>=3:
                col_nm=splitStr[0]
                col_type = splitStr[1]
                col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT  ", col_type)
                col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*', "STRING ",col_type)
                col_comm = splitStr[-1]
                col_comm = col_comm.replace(',', '')
                col_comm = col_comm.replace('\'', '')
                str1=','+col_nm.ljust(35)+col_type.ljust(20)+'COMMENT  '+ '\''+ col_comm + '\''
            elif len(splitStr) == 4:
                col_nm = splitStr[0]
                col_type = splitStr[1]
                col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT  ", col_type)
                col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*', "STRING ", col_type)
                str1 = ',' + col_nm.ljust(35) + col_type.ljust(20) + 'COMMENT  \'\' '
            else:
                col_nm = splitStr[0]
                str1 = col_nm.ljust(25)

            print str1

    print "\n"


    print "---------码值转换----------"

    for i in w:
        if len(i) > 1:
            splitStr = []
            splitStr = i.split()
            if len(splitStr) >= 1:
                col_nm1 = splitStr[0]
                # col_nm = splitStr[1]
                # col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT  ", col_type)
                # col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*', "STRING ", col_type)
                col_comm = splitStr[-1]
                col_comm = col_comm.replace(',', '')
                col_comm = col_comm.replace('\'', '')

                print (",T1.%-50s  AS  %-20s" % (col_nm1,col_nm1))



            # elif len(splitStr) == 4:
            #     col_nm = splitStr[0]
            #     print (",T1.%-50s  AS  %-50s " % (col_nm, col_nm))
            # else:
            #     col_nm = splitStr[0]
            #     print (",T1.%-50s  AS  %-50s " % (col_nm, col_nm))
            # # print str3




