 #!/bin/bash
##  echo "\t 1: ftp下载文件"      ods_interfact_ftp.sh
##  echo "\t 2: 解压下载文件"  	  ods_interfact_unzip.sh
##  echo "\t 3: 接口装载完成"     ods_interfact_load.sh
##如果FTP报错需要向本地数据服务器数据库写错误日志,并记录错误日志相关信息
###########################################################################################
#######################FTP函数,实现mget功能,单表进行ftp

#1、统计整合总体调度程序
print_usage()
{
  echo "Usage: ods_interface_runall.sh tablename "
  echo "$0" 
}
#2、从交换平台获取数据文件 FTP接口数据
sh_ods_interface_fpt()
{
echo  "begin $tablename ftp "
cd $workpath/shell
sh get_ftp_file.sh $tablename  $flag  $fdate
if [ $? != 0 ] ; then
    echo "ods_interface_fpt.sh failed!" 
    return 1
fi
}

#3、解压并检查合并接口文件
sh_ods_interface_unzip()
{
echo "begin $tablename ods_interface_unzip.sh"  
cd $workpath/shell
sh unzip.sh $tablename  $flag $fdate
if [ $? != 0 ] ; then
    echo "ods_interface_unzip.sh failed!"
    return 1
fi
}

#4、接口数据装载
sh_ods_interface_load()
{
echo "begin $tablename ods_interface_load.sh" 
cd $workpath/shell
sh load_file.sh $tablename $flag $fdate

if [ $? != 0 ] ; then
    echo "ods_interface_load.sh failed!" 
    return 1
fi

echo "end ods_interface_load.sh"  
date +"%Y-%m-%d %H:%M:%S"  
}

#5、整体调用
ods_interface_runall()
{
sh_ods_interface_fpt
if [ $? != 0 ] ; then
  echo "sh_ods_interface_fpt failed!" 
  exit -1
fi
sh_ods_interface_unzip
if [ $? != 0 ] ; then
  echo "sh_ods_interface_unzip failed!" 
  exit -1
fi

sh_ods_interface_load
if [ $? != 0 ] ; then
  echo "sh_dmbas_chk failed!" 
  exit -1
fi
}
#####################################
##for tablename in `cat ../ini/table_list.ini|awk '{print $2}'`
##do
##Example:sh ods_interface_judge $tablename i 2015-02-11 2015-02-25
##	  sh ods_interface_judge $tablename q 
if [ $# = 0 ] ; then
	echo "Please Input The Right Parameter!"
	exit -1
fi
tablename=$1
workpath=$(cd "$(dirname "$0")";cd ..;pwd)
##### 检测输入表名称是否正确
echo 'Input table name is '${tablename}
for tablename in `cat $workpath/ini/table_list.ini|grep -w $tablename|awk '{print $2}'`

        do maintable=`cat $workpath/ini/table_list.ini|grep -w  $tablename|awk '{print $1}'`
done
echo 'Truncate table '$maintable
if [ -z $maintable ]
then
        echo 'Input the right tablesname!Please!'
        exit
fi


flag=`cat $workpath/ini/wkr_date.ini|grep mode|awk '{print $2}'` #判断加载数据模式 i、q

shellconfigname="tmp$RANDOM`date "+%Y%m%d%H%M%S"`"
shellconfig=$workpath/shell/$shellconfigname
sh $workpath/shell/ods_decryption.sh $workpath/shell js_serverconfig.cfg $shellconfigname

db_server=`grep "v_db_server" $shellconfig | cut -d"=" -f2`  # Oracle 登陆字符串
rm $shellconfig
echo 'Begining......'



if [ $flag = i ]
then
fdate=`cat $workpath/ini/wkr_date.ini| grep 'D' |awk '{print $2}'`
filedate=`date -d ${fdate} +%Y%m%d`
end=`cat $workpath/ini/wkr_date.ini| grep 'D' |awk '{print $3}'`
enddate=`date -d ${end} +%Y%m%d`
echo 'fdate is '${fdate}
echo 'end IS '${end}
echo 'filedate is'$filedate
echo 'enddate is '$enddate
 
###### 按日期循环调用程序
for (( ; ; ))
do
	filedate=`date -d ${fdate} +%Y%m%d`
	enddate=`date -d ${end} +%Y%m%d`
	if [ ${filedate} -le ${enddate} ] ; then
		ods_interface_runall
#       fdate=$fdate+1	
        fdate=`date -d "$fdate  +1 day " +%Y%m%d`	
		echo " $tablename $filedatedate "I"  Task Complete!"
	else
		break
	fi
	done
elif [ $flag = q ];then
	fdate=`cat $workpath/ini/wkr_date.ini| grep 'M' |awk '{print $2}'`
	ods_interface_runall
       # fdate=`date -d $fdate +%Y%m` 
	echo " $tablename $filedate "Q"  Task Complete!"
fi

