# -*- coding: utf-8 -*-
import sys, var, os

reload(sys)
sys.setdefaultencoding('utf-8')
import re
from collections import defaultdict

import MySQLdb
import pandas as pd

"""
该utils类定义了三个方法：
1. getTableSchema 主要是通过连接MYSQL数据库，通过mysql的建表语句，通过拼接的方式，获取到 脚本中的 不同的表结构形式
2. createTablePython 主要用于生成 stg, ods , dwd层的三层的建表语句
3. createTablePython 主要用于生成 dwd 层的python 原始脚本
"""


def getTableSchema(tableName, dbName):
    engine = MySQLdb.connect(host='10.18.1.20', port=3306, user='liuyang', passwd='liuyang@2019', db=dbName,
                             connect_timeout=200, charset='utf8')

    createTable_sql = ''
    coalesce_sql = ''
    final_sql = ''
    table_nm = [tableName]
    for i in table_nm:
        sql = "show create table " + i
        df = pd.read_sql(sql, engine)
        content = df['Create Table'].values[0]
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

            for i in w:
                if len(i) > 1:
                    splitStr = i.split()
                    col_nm = splitStr[0]
                    col_type = splitStr[1]
                    col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT  ", col_type)
                    col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*',
                                      "STRING ", col_type)
                    col_comm = re.findall("'([^']*)'", i)
                    col_comm = ''.join(col_comm)
                    str1 = ",%-20s  %-20s COMMENT  '%-1s'" % (col_nm, col_type, col_comm)

                    createTable_sql = createTable_sql + str1 + '\n'

            for i in w:
                if len(i) > 1:
                    splitStr = []
                    splitStr = i.split()
                    col_nm = splitStr[0]
                    col_type = splitStr[1]
                    col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT  ", col_type)
                    col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*',
                                      "STRING ", col_type)
                    col_comm = re.findall("'([^']*)'", i)
                    col_comm = ''.join(col_comm)
                    col_type1 = col_type + ')'
                    str2 = ",CAST(T1.%-20s  AS  %-30s    AS %-30s --  %-20s" % (col_nm, col_type1, col_nm, col_comm)


                    coalesce_sql = coalesce_sql + str2 + '\n'

            for i in w:
                if len(i) > 1:
                    splitStr = []
                    splitStr = i.split()
                    col_nm = splitStr[0]
                    col_type = splitStr[1]
                    col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT  ", col_type)
                    col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*',"STRING ", col_type)
                    col_comm = re.findall("'([^']*)'", i)
                    col_comm = ''.join(col_comm)

                    str3 = (",T1.%-30s  AS  %-30s -- %-20s" % (col_nm, col_nm, col_comm))

                    final_sql = final_sql + str3 + '\n'
    # 对获取到的 建表语句，将 第一个 ， 剔除掉
    createTable_sql = ' ' + createTable_sql[1::1]
    coalesce_sql = ' ' + coalesce_sql[1::1]
    final_sql = ' ' + final_sql[1::1]
    return createTable_sql, coalesce_sql, final_sql


def createTablePython(targetPath, pyFile, stgTable, tableName1, odsTable, dwdTable, createTable_sql):
    if os.path.exists(targetPath):
        print ("targetPath: " + targetPath + " is exists\n")
    else:
        os.makedirs(targetPath)
        print ("create targetPath:" + targetPath + " sucess \n")

    with open(os.path.join(targetPath, pyFile), "w+") as fileWriter:
        fileWriter.write(var.Template_1)
        fileWriter.write(
            "    #--------------------------------------STG 建表------------------------------------------ \n\n")
        fileWriter.write("    #删除表 \n")
        fileWriter.write("    util.dropTable(DB_SOURCE_STG +  '." + stgTable + "'); \n\n")
        fileWriter.write("    sql = r\'\'\' \n")
        fileWriter.write("    CREATE TABLE IF NOT EXISTS $DB_SOURCE_STG$." + stgTable + "(\n\n")
        fileWriter.write(createTable_sql)
        fileWriter.write("\n        ) COMMENT '" + tableName1 + "' \n")
        fileWriter.write(var.Template_STG)
        fileWriter.write("        util.exit(ExitCode.EXIT_ERROR,'create table " + stgTable + " 异常出错') ; \n\n")
        fileWriter.write(
            "    #--------------------------------------ODS 建表------------------------------------------ \n\n")
        fileWriter.write("    #删除表 \n")
        fileWriter.write("    util.dropTable(DB_SOURCE_ODS +  '." + odsTable + "'); \n\n")
        fileWriter.write("    sql = r\'\'\' \n")
        fileWriter.write("    CREATE TABLE IF NOT EXISTS $DB_SOURCE_ODS$." + odsTable + "(\n\n")
        fileWriter.write(createTable_sql)
        fileWriter.write("\n        ) COMMENT '" + tableName1 + "' \n")
        fileWriter.write(var.Template_ODS)
        fileWriter.write("        util.exit(ExitCode.EXIT_ERROR,'create table " + odsTable + " 异常出错') ; \n\n")
        fileWriter.write(
            "    #--------------------------------------DWD 建表------------------------------------------ \n\n")
        fileWriter.write("    #删除表 \n")
        fileWriter.write("    util.dropTable(DB_SOURCE_DWD +  '." + dwdTable + "'); \n\n")
        fileWriter.write("    sql = r\'\'\' \n")
        fileWriter.write("    CREATE TABLE IF NOT EXISTS $DB_SOURCE_DWD$." + dwdTable + "(\n\n")
        fileWriter.write(createTable_sql)
        fileWriter.write(var.Template_DWD_0)
        fileWriter.write("\n        ) COMMENT '" + tableName1 + "' \n")
        fileWriter.write(var.Template_DWD)
        fileWriter.write("        util.exit(ExitCode.EXIT_ERROR,'create table " + dwdTable + " 异常出错') ; \n\n")
        fileWriter.write(var.Template_3)
        fileWriter.write(var.Template_4)
        fileWriter.close()


def createDWDPython(targetPath, pyFile, odsTable, dwdTable, createTable_sql, coalesce_sql, stgTable3, final_sql):
    if os.path.exists(targetPath):
        print ("targetPath: " + targetPath + " is exists\n")
    else:
        os.makedirs(targetPath)
        print ("create targetPath:" + targetPath + " sucess \n")
    with open(os.path.join(targetPath, pyFile), "w+") as fileWriter:
        fileWriter.write(var.Template_1)
        fileWriter.write("    # 映射字段函数 \n\n")
        fileWriter.write("    sql = r\'\'\' \n")
        fileWriter.write("     DROP TABLE  IF EXISTS $DB_TMP$." + dwdTable + "\n\n")
        fileWriter.write("    \'\'\' \n")
        fileWriter.write("    util.execSql(sql,locals())")
        fileWriter.write(var.Template_3 + "\n\n")
        fileWriter.write("    #建立临时表# \n")
        fileWriter.write("    sql = r\'\'\' \n")
        fileWriter.write("    CREATE TABLE IF NOT EXISTS $DB_TMP$." + dwdTable + "(\n\n")
        fileWriter.write(createTable_sql)
        fileWriter.write(var.Template_DWD_1)
        fileWriter.write("        util.exit(ExitCode.EXIT_ERROR,'建立临时表 " + dwdTable + " 异常出错') ;\n\n")
        fileWriter.write("    # Coalesce转换 \n")
        fileWriter.write(var.Template_DWD_2)
        fileWriter.write("            INSERT OVERWRITE TABLE $DB_TMP$." + dwdTable + "\n")
        fileWriter.write("            SELECT \n")
        fileWriter.write(coalesce_sql)
        fileWriter.write("                     ,CAST('" + stgTable3 + "'        AS STRING) --Src_Sysname-- \n")
        fileWriter.write("                     ,CAST('" + dwdTable + "'      AS STRING) --Src_Table-- \n")
        fileWriter.write("                     ,CAST('$bizDate$'         AS STRING) --Etl_Tx_Dt-- \n")
        fileWriter.write("                     ,CAST(CURRENT_TIMESTAMP() AS STRING) --Etl_Proc_Dt-- \n")
        fileWriter.write("                     ,CAST('0' AS STRING) --Etl_Del_Flag-- \n")
        fileWriter.write("        FROM          $DB_SOURCE_ODS$." + odsTable + " T1 \n")
        fileWriter.write(var.Template_DWD_3)
        fileWriter.write("        util.exit(ExitCode.EXIT_ERROR,'插入到" + dwdTable + " 异常出错') ; \n\n")
        fileWriter.write("    # 码值转换\n\n")
        fileWriter.write(var.Template_DWD_4)
        fileWriter.write("            INSERT OVERWRITE TABLE $DB_SOURCE_DWD$." + dwdTable + " PARTITION(Etl_Job,dt) \n")
        fileWriter.write("            SELECT \n")
        fileWriter.write(final_sql)
        fileWriter.write("                        ,CAST('" + stgTable3 + "'        AS STRING) --Src_Sysname-- \n")
        fileWriter.write("                        ,CAST('" + odsTable + "'     AS STRING)   -- 来源源系统表名 -- \n")
        fileWriter.write("                        ,CAST('$bizDate$'         AS STRING) --Etl_Tx_Dt-- \n")
        fileWriter.write("                        ,CAST(CURRENT_TIMESTAMP()         AS STRING)   -- Etl_Proc_Dt -- \n")
        fileWriter.write("                        ,CAST('ODS." + odsTable + "'  AS STRING)   --Etl_Job_Name-- \n")
        fileWriter.write("                        ,CAST('$bizDate$'                 AS STRING)   --Dt-- \n")
        fileWriter.write("            FROM    $DB_TMP$." + dwdTable + " T1 \n")
        fileWriter.write(var.Template_DWD_5 + "\n")
        fileWriter.write("    #删除临时表 \n")
        fileWriter.write("    sql = r\'\'\' \n")
        fileWriter.write("     DROP TABLE  IF EXISTS $DB_TMP$." + dwdTable + "\n\n")
        fileWriter.write("    \'\'\' \n")
        fileWriter.write("    util.execSql(sql,locals())")
        fileWriter.write(var.Template_3 + "\n\n")
        fileWriter.write(var.Template_4)
        fileWriter.close()
