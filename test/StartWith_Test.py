# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import re
from collections import defaultdict

import MySQLdb
import pandas as pd

if("DWD_THRD_TENCENT_FACE_RISK_I0100".startswith("DWD")):
    print "a"