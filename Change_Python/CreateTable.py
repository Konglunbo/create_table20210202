#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os, var, utils
import io
reload(sys);
sys.setdefaultencoding("utf8")

'''
获取列表中所有作业的文件，并将文件输出到指定路径

'''
sourcePath = '/home/edw/CreateTable/source/tableList.txt'
CreateTablePath = '/home/edw/CreateTable/target/createTable'
CreateDWDPath = '/home/edw/CreateTable/target/dwdTable'
tableList = {}
# 读取所有映射
with io.open(sourcePath, 'r+', encoding='utf-8') as tlist:
    for row in tlist:
        tables = row.split('\t')
        print  tables
        dbName = tables[0].upper()
        tableName = tables[1].upper()
        sqlTableName = tables[2].upper()
        stgTable = tables[3].upper()
        odsTable = tables[4].upper()
        dwdTable = tables[5].replace("\n",'').upper()

        print  dbName,tableName,sqlTableName,stgTable,odsTable,dwdTable
        stgTable2 = stgTable[2:]
        list = stgTable2.split("_")
        if list[0] == "S01":
            stgTable3 = stgTable[2:8]
        else:
            stgTable3 = stgTable[2:5]
        pyFile = stgTable2 + '_CREATE_TABLE.py'
        pyFile2 = stgTable2 + '_h0200.py'
        createTable_sql, coalesce_sql, final_sql = utils.getTableSchema(tableName, dbName)



        utils.createTablePython(CreateTablePath, pyFile, stgTable, sqlTableName, odsTable, dwdTable, createTable_sql)
        utils.createDWDPython(CreateDWDPath, pyFile2, odsTable, dwdTable, createTable_sql, coalesce_sql, stgTable3,
                              final_sql)
