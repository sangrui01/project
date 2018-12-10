#!/bin/bash
#This programe will put remote file to 10.1.14.7 by ftp to local
###########################################################################################                                                                      
##过程描述：本脚本实现从数据文件到接口表的装载,采取sqlldr技术，根据ftp下的文件记录数和装载成功数进行比对，判断是否装载成功                                 
##如果报错需要向本地数据服务器数据库写错误日志,并记录错误日志相关信息        
###########################################################################################
#####################写正常日志函数,并判断成功装载记录filecnt和ftp记录数fileftpcnt
etl_log(){
taskendtime=`date "+%Y-%m-%d %H:%M:%S"`
filecnt=`cat ../logs/${tablename}.log|grep Row|grep successfull|grep loaded|awk '{print $1}'` ##日志中加载成功的行数
echo "$filecnt:$fileftpcnt"
sqlplus $db_server << EOF
insert into etl_log(taskno, datadate, taskstarttime,maintblname,totalcnt,successcnt, taskstatus, taskendtime)values('${tablename}_load',to_date('$filedate1','yyyymmdd'),to_date('${taskstarttime}','yyyy-mm-dd hh24:mi:ss'),'${main_tablename}','${fileftpcnt}','${filecnt}','0',to_date('${taskendtime}','yyyy-mm-dd hh24:mi:ss'));
commit;
EOF
}
####################写错误日志函数,并解析错误日志,获取错误相关信息errmsg和errcode
etl_errlog(){
taskendtime=`date "+%Y-%m-%d %H:%M:%S"`
errmsg=`cat ../logs/${tablename}.log|egrep "Error|ORA|SQL*Loader"|awk 'END {print}'`
errcode=`cat ../logs/${tablename}.log|egrep "Error|ORA|SQL*Loader"|awk 'END {print}'|awk '{print $1}' |sed 's/://g'` ###modify by zzj,add:|sed 's/://g' 
sqlplus $db_server << EOF
insert into etl_errlog
(taskno, datadate, taskstarttime, errcode, errmessage,maintblname, taskstatus, taskendtime,errno)
values('${tablename}_load',to_date('$filedate1','yyyymmdd'),to_date('${taskstarttime}','yyyy-mm-dd hh24:mi:ss'),'${errcode}','${errmsg}','${main_tablename}','1',to_date('${taskendtime}','yyyy-mm-dd hh24:mi:ss'),'0');
commit;
EOF
}
######################判断成功装载记录filecnt和ftp记录数fileftpcnt的函数
get_loadcnt(){
echo "start get loadcont! tablename is "$tablename

filecnt=`cat ../logs/${tablename}.log|grep Row|grep successfull|grep loaded|awk '{print $1}'`
cd ../datafile
fileftpcnt_file=`ls | grep -w "dir.ods.*${tablename}_[q|d]\..*${filedate}.*"|sort -fn|tail -n 1` #控制文件，文件中包含数据量
fileftpcnt=`cat $fileftpcnt_file | awk '{print $3}'`
cd ../shell
}

####################参数初始化
workpath=$(cd "$(dirname "$0")";cd ..;pwd)  
cd $workpath/shell
if [ $# -eq 0 ]
	then
       echo "please input a  parameter tablename eg:ods_incr_polporf"
       exit -1
else
        main_tablename=$1
	flag=`cat $workpath/ini/wkr_date.ini|grep mode|awk '{print $2}'`	
fi
for tablename in `cat $workpath/ini/table_list.ini|grep $main_tablename|awk '{print $1}'`
do
	echo 'flag is ' $flag
	if [ $flag = i ];then
		filedate=$3
		filedate1=$3
		echo 'test 1' $filedate
	elif [ $flag = q ];then 
		filedate=`cat $workpath/ini/wkr_date.ini| grep 'M' |awk '{print $2}'`
		filedate1=$filedate"01"
		echo 'test 2' $filedate
	else 
		echo 'Please Input The Right Parameter!'
		echo 'test 3' $filedate
	fi
	echo 'filedate is '$filedate

	shellconfigname="tmp$RANDOM`date "+%Y%m%d%H%M%S"`"
	shellconfig=$workpath/shell/$shellconfigname
	sh $workpath/shell/ods_decryption.sh $workpath/shell js_serverconfig.cfg $shellconfigname
#######################数据库信息,数据日期,表名
#db_user=`grep "v_db_user" $shellconfig | cut -d"=" -f2`
#db_pass=`grep "v_db_pass" $shellconfig | cut -d"=" -f2`
	db_server=`grep "v_db_server" $shellconfig | cut -d"=" -f2`
	rm $shellconfig
	taskstarttime=`date "+%Y-%m-%d %H:%M:%S"`
###################开始装载
	echo "Load Start!"
	sqlldr $db_server control=$workpath/ddl/${tablename}.ctl log=$workpath/logs/${tablename}.log bad=$workpath/badfile/${tablename}.bad direct=Y  rows=10000 readsize=20680000 bindsize=20680000
	get_loadcnt
#######################判断成功装载记录filecnt和ftp记录数fileftpcnt为空处理,
#######################如果装载日志没有成功,则获取的装载成功日志为空，卸载记录和装载记录无法比较
#######################如果没有装载成功,则装载成功记录数直接置为0
	if [ -f  $workpath/logs/${tablename}.log ] ; then 
		filecntexsit=`cat $workpath/logs/${tablename}.log|grep Row|grep successfull|grep loaded|awk "{print '$1'}"`##从sqlldr的日志中获取加载成功的行数
		fileftpcntexsit=`cat $workpath/datafile/${tablename}.dat | wc -l` ##数据文件行数
	else 
		echo "${tablename}.log no exists!"
		filecnt=0
	fi
	if [ "$filecntexsit" = 0 ] ; then
		echo "${tablename}.log count: $filecntexsit,NULL! "
		filecnt=0
	fi
	if [ "$fileftpcntexsit" = 0 ] ; then
		echo "${tablename}.dat count: $fileftpcntexsit,NULL!"
		fileftpcnt=0
	fi
	if [ $filecnt -ne $fileftpcnt  ] ; then #filecnt日志文件成功加载行数，fileftpcnt
		echo "fileload failed,${tablename}.dat count not match ${tablename}.log count"
		echo "${tablename}.log count: $filecnt" 
		echo "${tablename}.dat count: $fileftpcnt"
		etl_errlog
		exit -1
	else
		etl_log
	fi
done
