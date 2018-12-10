#!/bin/bash
###########################################################################################                                                                    
##过程描述：本脚本实现从数据文件的解析以及解压合并工作                                 
##如果报错需要向本地数据服务器数据库写错误日志,并记录错误日志相关信息                   
###########################################################################################
#######################初始化脚本参数,读取日期配置表wkr_date.ini,开始日期和结束日期,
#######################第二列作为结束日期,结束日期作为装载日期
workpath=$(cd "$(dirname "$0")";cd ..;pwd) 
cd $workpath/datafile

shellconfigname="tmp$RANDOM`date "+%Y%m%d%H%M%S"`"
shellconfig=$workpath/shell/$shellconfigname
sh $workpath/shell/ods_decryption.sh $workpath/shell js_serverconfig.cfg $shellconfigname
############oracle 连接字符串

db_server=`grep "v_db_server" $shellconfig | cut -d"=" -f2`
rm $shellconfig
#######################开始时间
taskstarttime=`date "+%Y-%m-%d %H:%M:%S"`
if [ $# -eq 0 ]
then
       echo "please input a  parameter tablename eg:ods_incr_polporf"
       exit -1
else
        main_tablename=$1
	flag=$2
fi
etl_log(){
taskendtime=`date "+%Y-%m-%d %H:%M:%S"`
sqlplus $db_server << EOF
insert into etl_log
(taskno, datadate, taskstarttime,maintblname, taskstatus, taskendtime)
values('${filename}_unzip',to_date('$filedate1','yyyymmdd'),
to_date('${taskstarttime}','yyyy-mm-dd hh24:mi:ss'),'${main_tablename}','0',
to_date('${taskendtime}','yyyy-mm-dd hh24:mi:ss'));
commit;
EOF
}
#######################写错误日志函数
etl_errlog(){
taskendtime=`date "+%Y-%m-%d %H:%M:%S"`
sqlplus $db_server << EOF
insert into etl_errlog
(taskno, datadate, taskstarttime, errcode, errmessage,maintblname, taskstatus, taskendtime,errno)
values('${filename}_unzip',to_date('$filedate1','yyyymmdd'),
to_date('${taskstarttime}','yyyy-mm-dd hh24:mi:ss'),'0',
'No such file or directory or Not connected',
'${main_tablename}','1',to_date('${taskendtime}','yyyy-mm-dd hh24:mi:ss'),'0');
commit;
EOF
}
for filename in `cat $workpath/ini/table_list.ini|grep -w "$main_tablename" |awk '{print $1}'`
do
	if [ $flag = i ]
	then
	filedate=$3
	filedate1=$3
elif [ $flag = q ]
then
	filedate=`cat $workpath/ini/wkr_date.ini| grep '^M.*' |awk '{print $2}'`
	filedate1=$filedate"01"
else 
	echo 'Please Input The Rigt Parameter!'
fi
#######################如果存在多个数据文件,则取最新数据文件装载
ls  dir.ods.${filename}*${filedate}*|sort -fn |tail  -n  1 > $workpath/ini/${filename}.${filedate}.ini   ##sort 忽略大小写排序，取最新数据文件  
#######################为避免解压报错,首先判断数据文件是否已经存在,如存在则删除,否则执行解压操作
filesize=`ls | grep "ods.*${filename}.*${filedate}.*dat$"|wc -l`
echo "$filesize -- ods.*${filename}.${filedate}.*.dat"
if [ "$filesize" != 0 ] ; then
                        rm ods*${filename}*.${filedate}*dat 
			echo "DElETE!!"
fi
fileexsit=`ls | grep "ods.*${filename}.*${filedate}.*gz$"|wc -l`
echo "$fileexsit -- ods.${filename}.${filedate}.*.gz"
if [ "$fileexsit" != 0 ] ; then
	file_re_send=`ls | grep "ods.*${filename}.*${filedate}.*gz$"|sort -fn|tail -n 1` #对重传的表只取最新的文件    
	gzip -d ${file_re_send} 
	echo " ${file_re_send} re_send"
	if [ -f ${filename}.dat ] ; then
			rm ${filename}.dat
			echo "${filename}.dat"
	fi
	echo "unzip ${filename}.${filedate} success!"
	etl_log
else
	echo "unzip ${filename}.${filedate} failed!"
	etl_errlog
exit -1
fi
done

