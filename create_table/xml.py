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

        w.append(line.strip())

    print "\n"

    for i in w:
        # print (i)
        if len(i) >= 0:
            splitStr = []
            splitStr = i.split()
            if len(splitStr) >= 0:
                col_nm = splitStr[0]
                col_nm = col_nm.replace(',', '')
                #            map.put("ApplicationID",castNull.rmNull(map1.get("ApplicationID")) );
                # str1 = 'sb.append(map.get("' + col_nm + '").toString()).append("\\001");'
                # print str1
    #  map.put("als_m3_cell_cooff_orgnum", castNull.rmNull(m1.get("als_m3_cell_cooff_orgnum")));
    #            map.put("ApplicationID",castNull.rmNull(map1.get("ApplicationID")) );
                #map.put("﻿ApplicationID", castNull.rmNull(m1.get("﻿ApplicationID")));
                # str2= 'map.put("' + col_nm +  '", castNull.rmNull(map1.get("' + col_nm + '")));'
                # print str2

                col_comm = splitStr[1]
                col_comm = col_comm.replace(',', '')
                col_comm = col_comm.replace('\'', '')
                str3 = col_nm.ljust(35)  + 'STRING   COMMENT ' +'\''+col_comm+'\'' +','
                print  str3





