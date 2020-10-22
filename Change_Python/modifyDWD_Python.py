#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os

reload(sys);
sys.setdefaultencoding("utf8")

'''
获取列表中所有作业的文件，并将文件输出到指定路径

'''
# 脚本的原始路径
sourcePath = 'C:\\SVNwc\\source'
targetPath = 'C:\\SVNwc\\target'

if os.path.exists(targetPath):
    print ("targetPath: " + targetPath + " is exists\n")
else:
    os.makedirs(targetPath)
    print ("create targetPath:" + targetPath + " sucess \n")


def modifyPythons(sourcePath, pyFile):
    with open(os.path.join(sourcePath, pyFile), "r+") as fileReader:
        with open(os.path.join(targetPath, pyFile), "w+") as fileWriter:
            # 切分 pyFile, 以获取表名
            tableNameList = pyFile.split("_")[0:-1]
            # 通过拼接获取表名
            tableName = "_".join(tableNameList).upper()
            print("dwd表名：" + tableName)
            for row in fileReader:
                fileWriter.write(row)
                # 添加 删除两日前数据的 语句 ，dwd层 只保留2天的数据
                if (row.startswith("    #--------------------------------------SQL语句块【开始】")):
                    print row
                    fileWriter.write("    #  删除历史分区数据 \n")
                    fileWriter.write(
                        "    sql = \"ALTER TABLE DWD." + tableName + " DROP PARTITION ( DT < \" + dateByCount + \")\"  \n")
                    fileWriter.write("    print (sql + \'\\n\') " + " \n")
                    fileWriter.write("    util.execSql(sql,locals()) ")
        fileWriter.close()
    fileReader.close()


def getPathAllFiles(sourcePath, childPath):
    for tmpdirs in os.listdir(sourcePath):
        # 获取到脚本的具体路径后，进行拼接
        tmpPath = os.path.join(sourcePath, tmpdirs)
        print "------------" + tmpPath
        if os.path.isdir(tmpPath):
            # 通过递归获取到最终路径的文件
            childPath += "/" + tmpPath
            getPathAllFiles(os.path.join(sourcePath, tmpPath), childPath)
        else:
            # 如果没有子路径的情况下，对最终文件进行变更，进入到else语句执行逻辑
            print (tmpPath + "：：：：：变更")
            modifyPythons(sourcePath, tmpdirs)


getPathAllFiles(sourcePath, "")
