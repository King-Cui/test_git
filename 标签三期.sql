1.确定数据源
2.确定统计范围
3.确定计算逻辑
4.开发
5.mysql2hive 导数
6.开发测试
7.上线结束

-----------------------------------------------------------------------------------------


db_caizhi_manage.t_staff_customer_tag         caizhi_bigdata.dwd_staff_customer_tag_df 
db_caizhi_manage.t_corp_qywx_customer_tag     caizhi_bigdata.dwd_corp_qywx_customer_tag_df 

db_caizhi_manage.t_kyc_log_info               caizhi_bigdata.ods_kyc_log_info 
db_caizhi_manage.t_kyc_log_content

CREATE EXTERNAL TABLE `dwd_staff_customer_tag_df`(
`fid` bigint COMMENT '主键', 
`fstaff_id` string COMMENT '员工id', 
`fcustomer_id` string COMMENT '客户id', 
`ftag_id` bigint COMMENT '标签id', 
`ftype` int COMMENT '标签类型: 1. 预设标签 2. 自定义标签', 
`fis_deleted` int COMMENT '是否删除: 0. 未删除, 1. 已删除', 
`fcreate_time` string COMMENT '创建时间', 
`fupdate_time` string COMMENT '更新时间', 
`fcorp_id` string COMMENT '机构id', 
)

CREATE EXTERNAL TABLE `dwd_corp_qywx_customer_tag_df`(
`fid` bigint COMMENT '主键', 
`fcorp_id` string COMMENT '企业ID', 
`ftag_wxid` string COMMENT '标签在企微的id', 
`ftag_name` string COMMENT '标签名称', 
`fparent_id` int COMMENT '父标签(标签组)ID', 
`fparent_wxid` string COMMENT '父标签在企微的id', 
`fparent_name` string COMMENT '父标签(标签组)名称', 
`fservice_type` int COMMENT '标签的客户类型: 1. 对公, 2. 对私', 
`ftag_type` int COMMENT '1.标签组, 2.标签', 
`fis_deleted` int COMMENT '是否删除: 0. 未删除, 1. 已删除', 
`fcreate_time` timestamp COMMENT '创建时间', 
`fupdate_time` timestamp COMMENT '更新时间', 
`ftagging_type` int COMMENT '标签类型:0非群发，1群发')

select
 a1.fcorp_id,
 a1.fstaff_id,
 a1.fcustomer_id,
 a1.ftag_id,
 ftag_wxid,  --标签在企微的id
 ftag_name,  --标签名称
 fparent_id,  --父标签(标签组)ID
 fparent_wxid, --父标签在企微的id
 fparent_name, --父标签(标签组)名称
 fservice_type,  --标签的客户类型 
 ftag_type  --1.标签组, 2.标签' 
from
(
select 
fcorp_id,
fstaff_id,
fcustomer_id,
ftag_id
from caizhi_bigdata.dwd_staff_customer_tag_df
where fis_deleted = 0 and ftype = 1 
)a1
join
(
select
fid,
fcorp_id,
ftag_wxid,  --标签在企微的id
ftag_name,  --标签名称
fparent_id,  --父标签(标签组)ID
fparent_wxid, --父标签在企微的id
fparent_name, --父标签(标签组)名称
fservice_type,  --标签的客户类型 
ftag_type  --1.标签组, 2.标签' 
from caizhi_bigdata.dwd_corp_qywx_customer_tag_df
where fis_deleted = 0
)a2
on a1.fcorp_id = a2.fcorp_id and a1.ftag_id = a2.fid




SELECT * 
FROM caizhi_bigdata.ods_staff_client 
where fid = '7ca3da8f1ab04041b2df15f7e42aeab3'
limit  20

fid = fcustomer_id 



-------------------------------------------------------------------------------------------------
db_caizhi_manage.t_kyc_log_info               caizhi_bigdata.ods_kyc_log_info 
db_caizhi_manage.t_kyc_log_content            caizhi_bigdata.ods_kyc_log_content


CREATE TABLE `t_kyc_log_info` (
`Fid` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
`Fcorp_id` varchar(32) NOT NULL DEFAULT '' COMMENT '企业id',
`Fstaff_id` varchar(63) NOT NULL DEFAULT '' COMMENT '客户经理id',
`Fvisitor_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '访客在同一来源入口的唯一id',
`Fsource_type` int(11) NOT NULL DEFAULT '1' COMMENT '来源类型：1为个人名片',
`Ftitle` varchar(255) NOT NULL DEFAULT '' COMMENT '标题',
`Fkyc_column_id` bigint(20) DEFAULT NULL COMMENT 'kyc栏目id',
`Fis_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除 0为否，1为是',
`Fcreate_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`Fupdate_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
`Fcreate_by` varchar(63) NOT NULL DEFAULT '' COMMENT '创建人',
`Fupdate_by` varchar(63) NOT NULL DEFAULT '' COMMENT '修改人',
PRIMARY KEY (`Fid`),
KEY `index_query` (`Fcorp_id`,`Fstaff_id`,`Fsource_type`,`Fkyc_column_id`,`Fvisitor_id`) COMMENT '查询索引'
) ENGINE=InnoDB AUTO_INCREMENT=13099 DEFAULT CHARSET=utf8 COMMENT='kyc流水信息表'



CREATE TABLE `t_kyc_log_content` (
`Fid` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
`Fcorp_id` varchar(32) NOT NULL DEFAULT '' COMMENT '企业id',
`Fkyc_log_info_id` bigint(20) NOT NULL DEFAULT '0' COMMENT 'kyc流水信息id',
`Fquestion_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '问题id',
`Fquestion_content` varchar(255) NOT NULL DEFAULT '' COMMENT '问题内容',
`Fanswer_type` tinyint(4) NOT NULL DEFAULT '1' COMMENT '答案类型，1为单选，2为多选，3为纯文本',
`Fanswer_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '答案id',
`Fanswer_content` varchar(1024) NOT NULL DEFAULT '' COMMENT '答案内容',
`Fis_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除 0为否，1为是',
`Fcreate_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`Fupdate_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
`Fcreate_by` varchar(63) NOT NULL DEFAULT '' COMMENT '创建人',
`Fupdate_by` varchar(63) NOT NULL DEFAULT '' COMMENT '修改人',
PRIMARY KEY (`Fid`),
KEY `index_query` (`Fcorp_id`,`Fkyc_log_info_id`) COMMENT '查询索引'
) ENGINE=InnoDB AUTO_INCREMENT=133373 DEFAULT CHARSET=utf8 COMMENT='kyc流水内容表'




CREATE EXTERNAL TABLE `ods_kyc_log_info`(
`fid` bigint COMMENT '主键', 
`fcorp_id` string COMMENT '企业id', 
`fstaff_id` string COMMENT '用户id', 
`fvisitor_id` bigint COMMENT '访客在同一来源入口的唯一id', 
`fsource_type` int COMMENT '来源类型：1为个人名片', 
`ftitle` string COMMENT '标题', 
`fkyc_column_id` bigint COMMENT 'kyc栏目id', 
`fis_deleted` int COMMENT '是否删除 0为否，1为是', 
`fcreated_time` timestamp COMMENT '创建时间', 
`fupdated_time` timestamp COMMENT '修改时间', 
`fcreated_by` string COMMENT '创建人', 
`fupdated_by` string COMMENT '更新人')
COMMENT 'kyc流水信息表 '
PARTITIONED BY ( 
`dt` string)
ROW FORMAT SERDE 
'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
'field.delim'='\t', 
'serialization.format'='\t') 
STORED AS INPUTFORMAT 
'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
OUTPUTFORMAT 
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
'hdfs://HDFS44536/warehouse/ods/ods_kyc_log_info'

CREATE EXTERNAL TABLE `ods_kyc_log_content`(
`Fid` bigint COMMENT '主键',
`Fcorp_id` string COMMENT '企业id',
`Fkyc_log_info_id` bigint COMMENT 'kyc流水信息id',
`Fquestion_id` bigint COMMENT'问题id',
`Fquestion_content` string  COMMENT '问题内容',
`Fanswer_type` int  COMMENT '答案类型，1为单选，2为多选，3为纯文本',
`Fanswer_id` int COMMENT '答案id',
`Fanswer_content` string  COMMENT '答案内容',
`Fis_deleted` int COMMENT '是否删除 0为否，1为是',
`Fcreate_time` timestamp COMMENT '创建时间',
`Fupdate_time` timestamp COMMENT '更新时间',
`Fcreate_by` string  COMMENT '创建人',
`Fupdate_by` string  COMMENT '修改人')
COMMENT 'kyc流水内容表'
PARTITIONED BY (`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/ods/ods_kyc_log_content';




select
	`fid` bigint COMMENT '主键', 
	`fcorp_id` string COMMENT '企业id', 
	`fstaff_id` string COMMENT '用户id', 
	`fvisitor_id` bigint COMMENT '访客在同一来源入口的唯一id', 
	`fsource_type` int COMMENT '来源类型：1为个人名片', 
	`ftitle` string COMMENT '标题', 
	`fkyc_column_id` bigint COMMENT 'kyc栏目id', 
	`fis_deleted` int COMMENT '是否删除 0为否，1为是', 
	`fcreated_time` timestamp COMMENT '创建时间', 
	`fupdated_time` timestamp COMMENT '修改时间', 
	`fcreated_by` string COMMENT '创建人', 
	`fupdated_by` string COMMENT '更新人'
from caizhi_bigdata.ods_kyc_log_info 
where 



select 
	fcorp_id,
	fquestion_id,
	Fquestion_content,
	,
	`Fid` bigint COMMENT '主键',
	`Fcorp_id` string COMMENT '企业id',
	`Fkyc_log_info_id` bigint COMMENT 'kyc流水信息id',
	`Fquestion_id` bigint COMMENT'问题id',
	`Fquestion_content` string  COMMENT '问题内容',
	`Fanswer_type` int  COMMENT '答案类型，1为单选，2为多选，3为纯文本',
	`Fanswer_id` int COMMENT '答案id',
	`Fanswer_content` string  COMMENT '答案内容',
	`Fis_deleted` int COMMENT '是否删除 0为否，1为是',
	`Fcreate_time` timestamp COMMENT '创建时间',
	`Fupdate_time` timestamp COMMENT '更新时间',
	`Fcreate_by` string  COMMENT '创建人',
	`Fupdate_by` string  COMMENT '修改人'
from caizhi_bigdata.ods_kyc_log_content
where



--------------------------------finish-------------------
select
	a.fcorp_id as fcorp_id,
	fmain_id,
	level_1_category,
	level_2_category,
	label_name,
	tag_content
from
(
select fid,fcorp_id,Fkyc_log_info_id,fquestion_id,Fquestion_content,Fanswer_id,Fanswer_content
from caizhi_bigdata.ods_kyc_log_content
where Fanswer_type in (1,2) and fis_deleted = 0
and fquestion_id in (1,2,3,4,5,6)
) a 
join
(
select fid,fcorp_id,fvisitor_id,ftitle,fsource_type,fkyc_column_id
from caizhi_bigdata.ods_kyc_log_info 
where fis_deleted = 0 
) b
on a.Fkyc_log_info_id = b.fid and a.fcorp_id = b.fcorp_id
join
(
select fquestion_id,fanswer_id,level_1_category,level_2_category,label_name,tag_content
from caizhi_label.dm_questionnaire_label_routing
where fquestion_id in (1,2,3,4,5,6) 
) c
on a.fquestion_id = c.Fquestion_id and a.fanswer_id = c.fanswer_id
join 
(
SELECT fvisitor_id,fmain_id,fcorp_id
from caizhi_bigdata.dwd_visitor_df 
) d
on  b.fcorp_id = d.fcorp_id and b.fvisitor_id = d.fvisitor_id


create  table `dm_questionnaire_label_routing`(
`fquestion_id` string COMMENT '问题id',
`fquestion_content` string COMMENT '问题内容',
`fanswer_type` string COMMENT '问题	答案类型（1单选，2多选）',
`fanswer_id` string COMMENT '答案id',
`fanswer_content` string COMMENT '答案内容',
`level_1_category` string COMMENT '一级类目',
`level_2_category` string COMMENT '二级类目',
`label_name` string COMMENT '标签名称',
`tag_content` string COMMENT '标签内容'
)comment '问卷vo标签——路由表'
location '/warehouse/dm/dm_questionnaire_label_routing';




dws_kyc_questionnaire_label
														
CREATE EXTERNAL TABLE `dm_product_transition_staff_30days`(
`fcorp_id` string COMMENT '公司ID',
`fmain_id` string COMMENT '客户main_id',
`level_1_category` string COMMENT '一级类目',
`level_2_category` string COMMENT '二级类目',
`label_name` string COMMENT '标签名称',
`tag_content` string COMMENT '标签内容'
)COMMENT 'kyc问卷标签'
PARTITIONED BY (`dt` string)
LOCATION '/warehouse/dm/dm_product_transition_staff_30days';
