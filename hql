ddl
创建数据库
  create database test ;
删除数据库
  drop database if exists test;
创建表
  内部表：
   create table if not exists mydb.employees(
                 name string comment 'employee name',
				         salary float comment 'employee salary',
                 subordinates array<string> comment 'names of subordinates',
                 deductions map<string,float>,
                 address struct<street:string,city:string,state:string,zip:int> comment 'home address'
                 )
                 comment 'description of table'
                 tblproperties('creator'='me','date'='1231-21-31');
  外部表：
  create external table if not exists  stocks(
    exchange1 string,
    symbol string,
    ymd string,
    price_open float,
    price_high float, 
	  price_low float,
    price_close float,
    volumn int, 
    price_adj_close float
    )
	row format delimited 
  fields terminated by  ',';
	
  分区表：
  create external table if not exists log_messages(
    hms int,
    severity string,
    server string,
    process_id int,
    message string) 
    partitioned by(year int,month int,day int)
    row format delimited  fields terminated by '\t';
  
删除表
  drop table  if exists  employees;
修改表
  重命名
  alter table log_message rename to logmsgs;
  增加分区
  alter table log_messages add if not exists partition (year=2011,month=01,day=21);
  删除分区
  alter table log_messages drop if exists partition(year=2011,month=01,day=21);
  修改分区
  alter table log_messages partition(year=2011,month=01,day=21) set location '/logs/2018/01/01'
  增加列
  alter table log_message add columns(app_name string comment 'Application name',session_id string comment 'the current session id');
  修改表属性
  alter table log_message set tblproperties('notes'='xxxxxxxxxx')
  
  
  alter table log_message archive partition(year=2011,month=01,day=21)
  alter table log_message unarchive partition(year=2011,month=01,day=21) 
  alter table log_message partition(year=2011,month=01,day=21) enable no_drop
  alter table log_message partition(year=2011,month=01,day=21) enable offline
   
dml  
   
   
   
   
   
   
   
   
