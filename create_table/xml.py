# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import re
from collections import defaultdict

import MySQLdb
import pandas as pd






with open('C:\\SCRT\\DMA_PRD_DISTR_INFO_I_M.txt', 'r') as f:
    w = []
    for line in f.readlines():
        line = line.upper()
        w.append(line.strip())

    print "\n"

    for i in w:
        # print (i)
        if len(i) >= 0:
            splitStr = []
            splitStr = i.split("\t")
            event_type_list=splitStr[2].split(":")
            event_type=event_type_list[0]


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
                col_comm = col_comm.replace('\'', '').strip()
                str3 = col_nm.ljust(35)  + event_type.ljust(35) + '   COMMENT ' +'\''+col_comm+'\'' +','
                print  str3





