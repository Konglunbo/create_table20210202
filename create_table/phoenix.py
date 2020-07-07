# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import re
from collections import defaultdict

import MySQLdb
import pandas as pd






with open('C:\\SCRT\\a.txt', 'r') as f:
    w = []
    for line in f.readlines():
        line = line.lower()
        # print(line)
        w.append(line.strip())

    print "\n"

    print "---------Coalesce转换-----------"

    for i in w:
        # print (i)
        if len(i) >= 0:
            splitStr = []
            splitStr = i.split()
            if len(splitStr) >= 0:
                col_nm = splitStr[0]
                col_nm = col_nm.replace(',', '')
                col_nm = col_nm.replace('t1.', '')
                col_nm = col_nm.replace('`', '')
                str2 = ',CF1.' + col_nm + ' varchar'
                print str2

    print "\n"






