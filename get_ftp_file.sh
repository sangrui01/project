#!/bin/bash
###########################################################################################
##过程描述：本脚本实现从数据交换平台到本地服务器的下载，数据交换服务器：
##如果FTP报错需要向本地数据服务器数据库写错误日志,并记录错误日志相关信息
###########################################################################################
#######################FTP函数,实现mget功能,单表进行ftp
ftpfile(){
lftp -u ${serveruser},${serverpass} sftp://${serverip} <<!   ##modify by  sun 20160425  modify IP address
#cd  /odsdata/swap/data/ods/${filedate}
 cd ${remotedir}/${filedate}
mget  *${filename}*.*${filedate}.*
bye 
!
}   
#######################写正常日志函数
etl_log(){
sqlplus $db_server << EOF
insert into etl_log(taskno, datadate, taskstarttime,maintblname, taskstatus, taskendtime)values('${filename}_ftp',to_date('$filedate1','yyyymmdd'),
to_date('${taskstarttime}','yyyy-mm-dd hh24:mi:ss'),'${main_tablename}','0',to_date('${taskendtime}','yyyy-mm-dd hh24:mi:ss'));
commit;
EOF
}   
#######################写错误日志函数
etl_errlog(){
sqlplus $db_server << EOF
insert into etl_errlog
(taskno, datadate, taskstarttime, errcode, errmessage,maintblname, taskstatus, taskendtime,errno)
values('${filename}_ftp',to_date('$filedate1','yyyymmdd'),to_date('${taskstarttime}','yyyy-mm-dd hh24:mi:ss'),'0','No such file or directory or Not connected',
'${main_tablename}','1',to_date('${taskendtime}','yyyy-mm-dd hh24:mi:ss'),'0');
commit;
EOF
}  

#######################ftp前的准备,workpath为本地服务器默认目录,datafile目录为数据文本存放目录
workpath=$(cd "$(dirname "$0")";cd ..;pwd)
cd $workpath/datafile
#######################判断参数输入标准,一般本脚本需要输入接口表参数,如果参数输入有误,则程序退出
if [ $# -eq 0 ]
then
       echo "please input a  parameter tablename eg:ods_incr_polporf"
       exit -1
else
        main_tablename=$1
        flag=$2
fi
if [ $flag = i ]
then
	filedate=$3
	filedate1=$3   
elif [ $flag = q ]
then
	filedate=`cat $workpath/ini/wkr_date.ini| grep 'M' |awk '{print $2}'`
	filedate1=$filedate"01"
else
	echo "Inpunt The Right Parameter!"
	exit -1
fi

###########解密############
shellconfigname="tmp$RANDOM`date "+%Y%m%d%H%M%S"`"
shellconfig=$workpath/shell/$shellconfigname
sh $workpath/shell/ods_decryption.sh $workpath/shell js_serverconfig.cfg $shellconfigname
cd $workpath/datafile
#######################FTP服务器相关参数
serverip=`grep "v_serverip" $shellconfig | cut -d"=" -f2`
serveruser=`grep "v_serveruser" $shellconfig | cut -d"=" -f2`
serverpass=`grep "v_serverpass" $shellconfig | cut -d"=" -f2`
remotedir=`grep "v_remotedir" $shellconfig | cut -d"=" -f2`
#######################数据库信息,数据日期,表名
db_server=`grep "v_db_server" $shellconfig | cut -d"=" -f2`
rm $shellconfig
logfile=$workpath/logs/${filename}.${filedate}.log
echo "Starting FTP transfersing on "
taskstarttime=`date "+%Y-%m-%d %H:%M:%S"`
fileexsit=`cat $workpath/ini/table_list.ini|grep -w "$main_tablename" |awk '{print $1}'|wc -l`  ## grep -w 完全匹配
if [ "$fileexsit" = 0 ] ; then
	taskendtime=`date "+%Y-%m-%d %H:%M:%S"`
	echo ${filename}:$filedate:$taskstarttime:${main_tablename}:${taskendtime} 
	etl_errlog
	exit -1
fi
for filename in `cat $workpath/ini/table_list.ini|grep -w "$main_tablename" |awk '{print $1}'`  ## grep -w 完全匹配
do
########################执行ftp函数,并记录结束日期

ftpfile
taskendtime=`date "+%Y-%m-%d %H:%M:%S"` 
########################判断文件是否存在,并将相关日期记录到数据库中
filesize=`ls -lrt|grep -i "dir\..*${filename}.*\.${filedate}.*"|wc -l`  
if [ $filesize -eq  0 ] ; then 
	echo "ftpfile failed  "
	etl_errlog
	exit -1
else
	echo "Ftp Shell Running Successed!"
	etl_log
fi
done


