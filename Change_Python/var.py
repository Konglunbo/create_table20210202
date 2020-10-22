#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os

reload(sys);
sys.setdefaultencoding("utf8")


Template_1 = """#!/usr/local/python/bin/python
# coding=utf-8
################# HEAD START #################
# @descript：中信消金大数据模板 - I
# @author：zxxj
# @date: 2020-03-16
# @param： 
#   bizDate  业务日期  符合日期YYYYMMDD格式 
#
# @history
#

################# HEAD  END #################

# ------------模板说明-------------
# Python脚本模板V1
# 采用过程方式开发，基础组件提供调用接口，如接口不够，基础组件增加提供
#
#   1.建议采用Python Ide 工具开发 推荐PyCharm，PyDev
#   2.程序指定Python 2.7版本，目前不支持3.0以上版本
#   3.程序必须符合Python开发规范
#   4.所有实现方法必须在本文件内实现，原则上不允许自定义外部Python程序,如确有需要联系数仓项目组

import sys
import re
#from job.base.JobBase import ExitCode
#import job.base.ClientUtil as util
sys.path.insert(0,'/data/topcheer/app/jobBase')
from job.base.JobBase import ExitCode
import  job.base.ClientUtil as util

def checkArgs(length):
    util.debug('参数检查!')
    util.checkArgsEx(length);
try:

    #---------------------------------以上脚本信息不可以修改---------------------------------
    #--------------参数检查 参数赋值【开始】--------------
    # 参数检查 参数，当存续存在参数请预先检查 ，checkArgs(1) 参数是检查该方法外部参数有几个
    # 所有参数赋值，并提取各个参数值，如没有参数，请删除
    checkArgs(1)
    bizDate=sys.argv[1]
    #常量日期
    nullDate = '1900-01-02'
    minDate  = '1900-01-01'
    illDate  = '1900-01-03'
    maxDate  = '3000-12-31'
    bizDate2 = util.DateUtils.getDateByCount(bizDate,-2)
    #数据库名
    DB_TMP = 'TMP'
    DB_SOURCE_STG = 'STG';
    DB_SOURCE_ODS = 'ODS';
    DB_SOURCE_DWD = 'DWD';
    DB_SOURCE_DIM = 'DIM';
    DB_SOURCE_DWS = 'DWS';
    DB_SOURCE_ADS = 'ADS';


    #---------日期函数介绍---------
    #日期转换
    bizDate10 = util.DateUtils.getLongDate(bizDate)
    bizDate8 = util.DateUtils.getShortDate(bizDate10)
    #当前日期下一日
    nextDate = util.DateUtils.getNextDate(bizDate8)
    #当前日期上一日
    preDate = util.DateUtils.getPreDate(bizDate8)
    preDate10 = util.DateUtils.getLongDate(preDate)
    #当前日期对应周初（周一）
    weekStart=util.DateUtils.getWeekStart(bizDate8)
    #当前日期对应月初
    monthStart=util.DateUtils.getMonthStart(bizDate8)
    #当前日期上月初
    prMonStart=util.DateUtils.getPrMonStart(bizDate8)
    #当前日期对应旬初
    tendStart=util.DateUtils.getTendStart(bizDate8)
    #当前日期对应半年初
    halfyearStart=util.DateUtils.getHalfyearStart(bizDate8)
    #当前日期对应年初
    yearStart=util.DateUtils.getYearStart(bizDate8)
    #当前日期对应周末（周六）
    weekEnd=util.DateUtils.getWeekEnd(bizDate8)
    #当前日期对应月末
    monthEnd = util.DateUtils.getMonthEnd(bizDate8)
    #当前日期上月末
    prMonEnd=util.DateUtils.getPrMonEnd(bizDate8)
    #当前日期对应旬末
    tendEnd=util.DateUtils.getTendEnd(bizDate8)
    #当前日期对应季末
    quarterEnd = util.DateUtils.getQuarterEnd(bizDate8)
    #当前日期上季末
    prQtrEnd=util.DateUtils.getPrQtrEnd(bizDate8)
    #当前日期对应半年末
    halfyearEnd = util.DateUtils.getHalfyearEnd(bizDate10)
    #当前日期对应年末
    yearEnd=util.DateUtils.getYearEnd(bizDate8)
    #当前日期去年同期
    priSDate=util.DateUtils.getPriSDate(bizDate8)
    #当前日期前几天或后几天日期  当前日期为字符串格式为20150411
    dateByCount = util.DateUtils.getDateByCount(bizDate8,-2)
    #当前日期对应季初
    quarterStart=util.DateUtils.getQuarterStart(bizDate8)

    prMon= util.DateUtils.getPrMon(bizDate8,2)
  
    def getMapping(jobName):
        # 返回Mapping记录
        dbname,tbname = jobName.strip().split('.')
        sql = \"\"\"select dw_col_name,src_code,tar_code
                   from dim.dim_standard_code
                  where dw_tab_name = '%s' 
              \"\"\" % (tbname)
        res = util.execSql(sql,locals())
        if util._ERRORCODE:
            util.exit(ExitCode.EXIT_ERROR,'获取Mapping失败') ;
        return res

    def generateCaseWhen(value_list):
        field_mapping = {'VARCHAR':'','DECIMAL':0,'INTEGER':0}
        if value_list in (None,[]):
            return None
        else:
            dict1 = {}
            dict2 = {}
            for i in value_list:
                k,src_value,tgt_value = i
      
                if dict1.has_key(k):
                    dict1[k] = dict1[k] + " when T1.%s ='%s' then '%s'" % (k,src_value,tgt_value)
                else:
                    dict1[k] = "case when T1.%s ='%s' then '%s'" % (k,src_value,tgt_value)
                    # dict2[k] = ",CAST (%s AS %s))" % (k,tgt_type)
                    dict2[k] = " else 'ERR' END"
    
        res = {}
        for k in dict2.keys():
            res[k] = dict1[k] + dict2[k]
        return res


    #--------------参数检查 参数赋值【结束】--------------
   
    #--------------------------------------SQL语句块【开始】------------------------------------------

"""
Template_STG= """        PARTITIONED BY (
         DT STRING  COMMENT '数据日期'
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
        STORED AS TEXTFILE
        '''
    util.execSql(sql,locals())

    if util._ERRORCODE or util._ROWNUM==0:
"""

Template_ODS= """        PARTITIONED BY (
         DT STRING  COMMENT '数据日期'
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
        STORED AS PARQUET
        '''
    util.execSql(sql,locals())

    if util._ERRORCODE or util._ROWNUM==0:
"""

Template_DWD= """        PARTITIONED BY (
         ETL_JOB STRING  COMMENT 'ETL加载作业名'
         ,DT STRING COMMENT '数据日期'
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
        STORED AS PARQUET
        '''
    util.execSql(sql,locals())

    if util._ERRORCODE or util._ROWNUM==0:
"""


Template_DWD_0= """            ,SRC_SYSNAME         STRING         COMMENT '来源源系统代码'
            ,SRC_TABLE           STRING         COMMENT '来源源系统表名'
            ,ETL_TX_DT           STRING         COMMENT 'ETL业务日期' 
            ,ETL_PROC_DT         STRING         COMMENT 'ETL处理时间戳' 
"""


Template_DWD_1= """                ,SRC_SYSNAME         STRING         COMMENT '来源源系统代码'
                ,SRC_TABLE           STRING         COMMENT '来源源系统表名'
                ,ETL_TX_DT           STRING         COMMENT 'ETL业务日期'
                ,ETL_PROC_DT         STRING         COMMENT 'ETL处理时间戳'
                ,ETL_DEL_FLAG        STRING          COMMENT '数据删除标志'
        )
        STORED AS PARQUET
    '''
    util.execSql(sql,locals())

    if util._ERRORCODE or util._ROWNUM==0:
"""


Template_DWD_2= """
    sql = r'''
            SET hive.merge.mapfiles = true;
            SET hive.merge.mapredfiles = true;
            SET hive.merge.size.per.task = 256000000;
            SET hive.merge.smallfiles.avgsize = 134217728;
"""


Template_DWD_3= """        WHERE          T1.DT=CAST('$bizDate$'  AS STRING)
    '''
    util.execSql(sql,locals())
    if util._ERRORCODE:
"""

Template_DWD_4= """    sql = r'''
            SET hive.exec.dynamic.partition=true;
            SET hive.exec.dynamic.partition.mode=nonstrict; 
            SET parquet.memory.min.chunk.size=100000;
            SET hive.exec.max.dynamic.partitions=100000;
            SET hive.exec.max.dynamic.partitions.pernode=100000;
"""

Template_DWD_5= """    \'\'\'
    
    tmpStr=re.findall(r"res\[\'(\w+)\'\]",sql)
    for i in list(range(0,len(tmpStr))):
        sql=sql.replace("res['"+tmpStr[i]+"']",res[tmpStr[i]])
    util.execSql(sql,locals())

    if util._ERRORCODE:
        util.exit(ExitCode.EXIT_ERROR,'重写增量数据异常出错') ;
"""


Template_3= """
    if util._ERRORCODE:
        util.exit(ExitCode.EXIT_ERROR,'执行SQL失败') ;
"""

Template_4= """
    #--------------------------------------SQL语句块【结束】------------------------------------------

    #----------------------------以下脚本不可以修改---------------------------------
    util.exit(ExitCode.EXIT_SUCCESS,'执行成功') ;
except Exception as e:
        util.exit(ExitCode.EXIT_ERROR,'异常出错:'+e.message) ;
finally:
    util.destory()
"""


