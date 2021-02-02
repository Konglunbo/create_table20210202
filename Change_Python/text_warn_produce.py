#!/use/bin/python
# coding=utf-8
import urllib
import urllib2
import time
import hashlib
import MySQLdb
import time, datetime
import sys

 
reload(sys);
sys.setdefaultencoding('utf-8')
def md5(str):
    import hashlib
    m = hashlib.md5()
    m.update(str)
    return m.hexdigest()

def send_error_text(error_msg):

    url = 'http://124.251.7.68:8000/HttpQuickProcess/submitMessageAll'  # 接口地址
    timenew = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time()))
    pwd = 'qmWvEXzQ' 
#pwd = md5('qmWvEXzQ' + timenew)  # 密码
    print ("error_msg--------",error_msg)
    error_msg1 = error_msg.encode("gbk")

    values = {'OperID': 'zxxfss', 'OperPass': pwd, 'Content': error_msg1, 'DesMobile': '15222310846'}  # 开通的用户名
    print("values-------------",values)
    data = urllib.urlencode(values)
    print("data-------------",data)
    req = urllib2.Request(url, data)
    response = urllib2.urlopen(req)
    the_page = response.read()
    print the_page
print("开始运行!!!")
#exec_id_last=0
# 打开数据库连接  url,username,password,database
db = MySQLdb.connect("10.18.1.16","azkaban","UUT6SdhEk$v$lclo","azkaban" )
cursor = db.cursor()
cursor.execute("SELECT * FROM execution_jobs limit 1")
data_all = cursor.fetchall()
#总报错job个数
count= len(data_all)
if count==0:
	exit(0);
else:
	#第一条报错信息
	data = data_all[0]
	print data
	exec_id=data[0]
	job_id=data[4]
	end_time=data[7]
	end_time01 = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(end_time/1000))
	
	#预警信息
	error_msg="【调度异常】\n"+job_id+"等"+str(count)+"条任务调度异常"+"\n执行时间:  "+end_time01
	print(error_msg)
        #同一个exec_id 预警一次
        f2 = open('C:\\tmp\\exec_id.txt', 'r+')
        exec_id_last=f2.read().strip();
        print exec_id_last;
	print ("-------exec-id-------\n")
        print type(str(exec_id));
        print type(exec_id_last)
        print len(str(exec_id))
        print len(exec_id_last)
	#同一个exec_id 预警一次
	#f2 = open('/home/edw/text_warn/exec_id.txt', 'r+')
	#exec_id_last=f2.read()
	#print exec_id_last;
	print (str(exec_id) == exec_id_last)
	if str(exec_id)==exec_id_last:
		send_error_text(error_msg)
		f2 = open('C:\\tmp\\exec_id.txt', 'r+')
		f2.write(str(exec_id))
	# 关闭数据库连接
	db.close()


