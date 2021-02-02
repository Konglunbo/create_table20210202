#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os
import re
reload(sys);
sys.setdefaultencoding("utf8")


# 脚本的原始路径
sourcePath = '/home/edw/CreateTable/source'
targetPath = '/home/edw/CreateTable/target'

if os.path.exists(targetPath):
    print ("targetPath: "+targetPath + " is exists\n")
    os.rmdir(targetPath)
    os.mkdir(targetPath)
else:
    os.makedirs(targetPath)
    print ("create targetPath:"+targetPath + " sucess \n")


def modifyPythons(sourcePath,pyFile,newFile):
    createPath = sourcePath.replace('source','target')
    createPath = createPath.replace('ADS_PUSH_FXQ_HS_LM_SETLMT_LOG_S_D',newFile)
    print("createPath",createPath)

    if os.path.exists(createPath):
        print ("targetPath: " + targetPath + " is exists\n")
    else:
        os.makedirs(createPath)
        print ("create targetPath:" + createPath + " sucess \n")


    newFile1 = pyFile.replace('ADS_PUSH_FXQ_HS_LM_SETLMT_LOG_S_D',newFile)
    with open(os.path.join(sourcePath,pyFile), "r+") as fileReader:
        with open(os.path.join(createPath,newFile1), "w+") as fileWriter:
            print("pyFile",pyFile)
            print("os.path.join(createPath,newFile1)",os.path.join(createPath,newFile1))
            for row in fileReader:
                row = row.replace("ADS_PUSH_FXQ_HS_LM_SETLMT_LOG_S_D", newFile)
                # print row
                fileWriter.write(row)
        fileReader.close()




def getPathAllFiles(sourcePath,childPath,newFile):
    for tmpdirs in os.listdir(sourcePath):
        tmpPath = os.path.join(sourcePath,tmpdirs)
        print "------------" + tmpPath
        if os.path.isdir(tmpPath):
            childPath += "/" + tmpPath
            getPathAllFiles(os.path.join(sourcePath,tmpPath),childPath,newFile)
        else:
            print (tmpPath+"：：：：：变更")
            modifyPythons(sourcePath,tmpdirs,newFile)






with open('/home/edw/CreateTable/a.txt', 'r') as f:
    w = []
    for line in f.readlines():
        line = line.upper()
        w.append(line.strip())


    for i in w:
        getPathAllFiles(sourcePath, "",i)










