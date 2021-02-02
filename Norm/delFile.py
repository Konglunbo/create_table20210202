#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os,shutil,re
import io
reload(sys);
sys.setdefaultencoding("utf8")


"""
 Python批量删除指定文件夹下的指定文件名的文件
"""

sourcePath ="C:\\SVNwc\\source\\tableList.txt"

"""
获取指定目录下的所有文件名
"""
def file_name(file_dir):
    list = []
    for root, dirs, files in os.walk(file_dir):
        for file in files:
            list.append(file)
    return list

def shanchu(path):
#查看目录下所有文件或者文件夹
    s = os.listdir(path)
    for i in s:
    #拼接新的路径
        m_path = os.path.join(path, i)
        #如果是文件夹递归进入
        if os.path.isdir(m_path):
            shanchu(m_path)
        else:
        #如何不是拼接后删除
            os.remove(os.path.join(path, i))
    os.rmdir(os.path.join(path))

def delEndFile(path,filename):
    with open(path, 'r+') as f:
        # 读取文件并保存到text变量中
        text = f.read()
        # 替换 要删除的表名
        new_text = re.sub(filename, "", text)
        # 因为之前有读取过文件，所在在写入前需要先调整文件指针到初始位置
        f.seek(0)
        # 清空表内容
        f.truncate()
        f.write(new_text)







"""
    1、先读取到文件中STG层的表名，通过STG层表名的切割，获取层和表名
    2、通过获取层和表名 去 删除文件和路径
"""

# 删除ODS层对应的调度文件
def delODS(sourcePath):
    with open(sourcePath, 'r+') as tlist:
        for stgTable in tlist:
            # S_S01_QZ_LM_COMPENSATORY_LOG
            stgTable = stgTable.rstrip().upper()
            # 通过分隔符获取 S01 S02 作为判断
            list = stgTable.split("_")
            # list[0] S01
            if list[1] == "S01" or list[1] == "S03":
                # 获取层级名字  S01_HS S03_RD
                stgTable3 = stgTable[2:8]
                # 获取表名
                tableName = stgTable[9:]
            else:
                # 获取层级名字  S02 S04 S08
                stgTable3 = stgTable[2:5]
                # 获取表名
                tableName = stgTable[6:]
            print  stgTable3
            print tableName
            shanchu(r'C:\Users\kongfanxin.CITICCFC\Desktop\APP\ODS' + '\\'  + stgTable3 + '\\'  +  tableName)
            #删除 Ctrl 文件里的依赖
            # fileName = "ODS_" + stgTable3 + "_END.job"
            # path = r'C:\Users\kongfanxin.CITICCFC\Desktop\ETL_ODS_0107\CTRL' + '\\'  + fileName
            # file = "ODS_" + stgTable[2:] +  '_A0200,'
            # print path
            # print file
            # delEndFile(path,file)




# 删除DWD层对应的调度文件
def delDWD(sourcePath):
    with open(sourcePath, 'r+') as tlist:
        for dwdTable in tlist:
            # DWD_ACCT_PROD_ACCT_INFO_S
            dwdTable = dwdTable.rstrip().upper()
            # ['DWD', 'ACCT', 'PROD', 'ACCT', 'INFO', 'S']
            list = dwdTable.split("_")
            # ACCT
            catalog = list[1]
            # PROD_ACCT_INFO
            tableName = dwdTable[9:-2]
            shanchu(r'C:\Users\kongfanxin.CITICCFC\Desktop\APP\DWD' + '\\'  + catalog + '\\'  +  tableName)
            # #删除 Ctrl 文件里的依赖
            fileName = "DWD_" + catalog + "_END.job"
            path = r'C:\Users\kongfanxin.CITICCFC\Desktop\ETL_DWD_0107\CTRL' + '\\'  + fileName
            file = "DWD_" + dwdTable[4:] + "0100,"
            print path
            print file
            # delEndFile(path,file)


# 删除DWD层对应的调度文件 全命名文件
def delDWDFull(sourcePath):
    with open(sourcePath, 'r+') as tlist:
        for dwdTable in tlist:
            # DWD_ACCT_PROD_ACCT_INFO_S
            dwdTable = dwdTable.rstrip().upper()
            # ['DWD', 'ACCT', 'PROD', 'ACCT', 'INFO', 'S']
            list = dwdTable.split("_")
            # ACCT
            catalog = list[1]
            # PROD_ACCT_INFO
            tableName = dwdTable[9:-2]
            shanchu(r'C:\Users\kongfanxin.CITICCFC\Desktop\APP\DWD' + '\\'  + catalog + '\\'  +  dwdTable)
            # #删除 Ctrl 文件里的依赖
            fileName = "DWD_" + catalog + "_END.job"
            path = r'C:\Users\kongfanxin.CITICCFC\Desktop\ETL_DWD_0107\CTRL' + '\\'  + fileName
            file = "DWD_" + dwdTable[4:] + "0100,"
            # print path
            # print file
            # delEndFile(path, file)


# 主程序
if __name__ == "__main__":
    delDWDFull(sourcePath)








