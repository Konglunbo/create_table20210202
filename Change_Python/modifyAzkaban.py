#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os

reload(sys);
sys.setdefaultencoding("utf8")

'''
获取列表中所有作业的文件，并将文件输出到指定路径

'''
# 脚本的原始路径
sourcePath = '/home/edw/CreateTable/source'
targetPath = '/home/edw/CreateTable/target'

if os.path.exists(targetPath):
    print ("targetPath: "+targetPath + " is exists\n")
else:
    os.makedirs(targetPath)
    print ("create targetPath:"+targetPath + " sucess \n")


def modifyPythons(sourcePath,pyFile):
    createPath = sourcePath.replace('source','target')

    if os.path.exists(createPath):
        print ("targetPath: " + targetPath + " is exists\n")
    else:
        os.makedirs(createPath)
        print ("create targetPath:" + createPath + " sucess \n")

    with open(os.path.join(sourcePath,pyFile), "r+") as fileReader:
        with open(os.path.join(createPath,pyFile), "w+") as fileWriter:
            for row in fileReader:
                print row
                if(row.startswith("command") and "echo" not in row):
                    row = row.replace("\r\n","\n")
                    row = row.replace("\n", " ${data_dt} \n")
                print row
                fileWriter.write(row)
        fileReader.close()



def getPathAllFiles(sourcePath,childPath):
    for tmpdirs in os.listdir(sourcePath):
        tmpPath = os.path.join(sourcePath,tmpdirs)
        print "------------" + tmpPath
        if os.path.isdir(tmpPath):
            childPath += "/" + tmpPath
            getPathAllFiles(os.path.join(sourcePath,tmpPath),childPath)
        else:
            print (tmpPath+"：：：：：变更")
            modifyPythons(sourcePath,tmpdirs)



getPathAllFiles(sourcePath,"")








