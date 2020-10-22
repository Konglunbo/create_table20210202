# -*- coding: utf-8 -*-
import sys,var,os

reload(sys)
sys.setdefaultencoding('utf-8')
import re
from collections import defaultdict

import MySQLdb
import pandas as pd


def getTableSchema(tableName,dbName):
    engine = MySQLdb.connect(host='10.18.1.20', port=3306, user='liuyang', passwd='liuyang@2019', db=dbName,
                             connect_timeout=200, charset='utf8')

    col_nm = defaultdict(list)

    # 'cust_acct_prod_info','cust_info',
    # lm_loan_install_detl
    createTable_sql = ''
    coalesce_sql = ''
    final_sql = ''
    table_nm = [tableName]
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
                line = line.upper()
                # print(line)
                line = re.sub(r'`RS', "`ODS.ODS_RS", line)
                line = re.sub(r'` \(', " ` (", line)
                line = re.sub(r'PRIMARY.*$|UNIQUE.*$|KEY.*$|CREATE TABLE.*$|ENGINE=INNODB.*$', "", line)
                w.append(line.strip())

            for i in w:
                if len(i) > 1:
                    splitStr = []
                    splitStr = i.split()
                    if len(splitStr) >= 5:
                        col_nm = splitStr[0]
                        col_type = splitStr[1]
                        col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT  ", col_type)
                        col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*',
                                          "STRING ", col_type)
                        col_comm = splitStr[-1]
                        col_comm = col_comm.replace(',', '')
                        col_comm = col_comm.replace('\'', '')
                        str1 = ',' + col_nm.ljust(35) + col_type.ljust(20) + 'COMMENT  ' + '\'' + col_comm + '\''
                    elif len(splitStr) == 4:
                        col_nm = splitStr[0]
                        col_type = splitStr[1]
                        col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT  ", col_type)
                        col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*',
                                          "STRING ", col_type)
                        str1 = ',' + col_nm.ljust(35) + col_type.ljust(20) + 'COMMENT  \'\' '
                    else:
                        col_nm = splitStr[0]
                        str1 = col_nm.ljust(25)

                    createTable_sql = createTable_sql + str1 + '\n'

            for i in w:
                if len(i) > 1:
                    splitStr = []
                    splitStr = i.split()
                    if len(splitStr) >= 5:
                        col_nm = splitStr[0]
                        col_type = splitStr[1]
                        col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT  ", col_type)
                        col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*',
                                          "STRING ", col_type)
                        col_comm = splitStr[-1]
                        col_comm = col_comm.replace(',', '')
                        col_comm = col_comm.replace('\'', '')
                        col_type1 = col_type + ')'
                        str2 = ",CAST(T1.%-40s  AS  %-50s    AS %-50s --  %-50s" % (
                        col_nm, col_type1, col_nm, col_comm)

                    elif len(splitStr) == 4:
                        col_nm = splitStr[0]
                        col_type = splitStr[1]
                        col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT  ", col_type)
                        col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*',
                                          "STRING ", col_type)
                        col_type1 = col_type + ')'
                        str2 = ",CAST(T1.%-40s  AS  %-50s    AS %-50s --  %-50s" % (
                        col_nm, col_type1, col_nm, col_comm)
                    else:
                        col_nm = splitStr[0]
                        str2 = col_nm.ljust(25)

                    coalesce_sql = coalesce_sql + str2 + '\n'

            for i in w:
                if len(i) > 1:
                    splitStr = []
                    splitStr = i.split()
                    if len(splitStr) >= 5:
                        col_nm = splitStr[0]
                        col_type = splitStr[1]
                        col_type = re.sub(r'BIGINT.*|TINYINT.*|INT.*', "BIGINT  ", col_type)
                        col_type = re.sub(r'VARCHAR.*|DATETIME.*|CHAR.*|TIMESTAMP.*|LONGTEXT.*|DATE.*|TEXT.*',
                                          "STRING ", col_type)
                        col_comm = splitStr[-1]
                        col_comm = col_comm.replace(',', '')
                        col_comm = col_comm.replace('\'', '')

                        str3 = (",T1.%-50s  AS  %-50s -- %-50s" % (col_nm, col_nm, col_comm))

                    elif len(splitStr) == 4:
                        col_nm = splitStr[0]
                        str3 = (",T1.%-50s  AS  %-50s -- %-50s" % (col_nm, col_nm, col_comm))
                    else:
                        col_nm = splitStr[0]
                        str3 = col_nm.ljust(25)

                    final_sql = final_sql + str3 + '\n'

    createTable_sql = ' ' + createTable_sql[1::1]

    coalesce_sql = ' ' + coalesce_sql[1::1]
    final_sql = ' ' + final_sql[1::1]

    return createTable_sql, coalesce_sql, final_sql

def createTablePython(targetPath,pyFile,stgTable,tableName1,odsTable,dwdTable,createTable_sql):
    if os.path.exists(targetPath):
        print ("targetPath: " + targetPath + " is exists\n")
    else:
        os.makedirs(targetPath)
        print ("create targetPath:" + targetPath + " sucess \n")

    with open(os.path.join(targetPath, pyFile), "w+") as fileWriter:
        fileWriter.write(var.Template_1)
        fileWriter.write("    #--------------------------------------STG 建表------------------------------------------ \n\n")
        fileWriter.write("    #删除表 \n")
        fileWriter.write("    util.dropTable(DB_SOURCE_STG +  '." +stgTable+"'); \n\n")
        fileWriter.write("    sql = r\'\'\' \n")
        fileWriter.write("    CREATE TABLE IF NOT EXISTS $DB_SOURCE_STG$." +stgTable +"(\n\n")
        fileWriter.write(createTable_sql)
        fileWriter.write("\n        ) COMMENT '" +tableName1+"' \n")
        fileWriter.write(var.Template_STG )
        fileWriter.write("        util.exit(ExitCode.EXIT_ERROR,'create table " +stgTable +" 异常出错') ; \n\n")
        fileWriter.write("    #--------------------------------------ODS 建表------------------------------------------ \n\n")
        fileWriter.write("    #删除表 \n")
        fileWriter.write("    util.dropTable(DB_SOURCE_ODS +  '." +odsTable+"'); \n\n")
        fileWriter.write("    sql = r\'\'\' \n")
        fileWriter.write("    CREATE TABLE IF NOT EXISTS $DB_SOURCE_ODS$." +odsTable +"(\n\n")
        fileWriter.write(createTable_sql)
        fileWriter.write("\n        ) COMMENT '" +tableName1+"' \n")
        fileWriter.write(var.Template_ODS )
        fileWriter.write("        util.exit(ExitCode.EXIT_ERROR,'create table " +odsTable +" 异常出错') ; \n\n")
        fileWriter.write("    #--------------------------------------DWD 建表------------------------------------------ \n\n")
        fileWriter.write("    #删除表 \n")
        fileWriter.write("    util.dropTable(DB_SOURCE_DWD +  '." +dwdTable+"'); \n\n")
        fileWriter.write("    sql = r\'\'\' \n")
        fileWriter.write("    CREATE TABLE IF NOT EXISTS $DB_SOURCE_DWD$." +dwdTable +"(\n\n")
        fileWriter.write(createTable_sql)
        fileWriter.write(var.Template_DWD_0)
        fileWriter.write("\n        ) COMMENT '" +tableName1+"' \n")
        fileWriter.write(var.Template_DWD )
        fileWriter.write("        util.exit(ExitCode.EXIT_ERROR,'create table " +dwdTable +" 异常出错') ; \n\n")
        fileWriter.write(var.Template_3)
        fileWriter.write(var.Template_4)
        fileWriter.close()

def createDWDPython(targetPath,pyFile,odsTable,dwdTable,createTable_sql,coalesce_sql,stgTable3,final_sql):
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

