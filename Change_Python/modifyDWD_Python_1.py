#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os

reload(sys);
sys.setdefaultencoding("utf8")

'''
该脚本主要是针对征信表上线进行开发
主要是用来对DWD层的脚本 加上删除旧分区的逻辑SQL语句，修改dateby 语句 控制保存一天
'''
# 脚本的原始路径
sourcePath = '/home/edw/kongfanxin/source'
targetPath = '/home/edw/kongfanxin/target'

if os.path.exists(targetPath):
    print ("targetPath: " + targetPath + " is exists\n")
else:
    os.makedirs(targetPath)
    print ("create targetPath:" + targetPath + " sucess \n")


def modifyPythons(sourcePath, pyFile):
    createPath = sourcePath.replace('source','target')

    if os.path.exists(createPath):
        print ("targetPath: " + targetPath + " is exists\n")
    else:
        os.makedirs(createPath)
        print ("create targetPath:" + createPath + " sucess \n")
    with open(os.path.join(sourcePath, pyFile), "r+") as fileReader:
        with open(os.path.join(createPath, pyFile), "w+") as fileWriter:
            for row in fileReader:
                # 添加 删除两日前数据的 语句 ，dwd层 只保留2天的数据
                if (row.startswith("    dateByCount = util.DateUtils.getDateByCount(bizDate8,-2)")):
                    print row
                    row = row.replace("    dateByCount = util.DateUtils.getDateByCount(bizDate8,-2)", "    dateByCount = util.DateUtils.getDateByCount(bizDate8,-0)")
                    print row
                    fileWriter.write(row)
                else:
                    fileWriter.write(row)

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
