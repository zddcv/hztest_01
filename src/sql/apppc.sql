--1. 将数据导入dwd_user_app_pv
insert overwrite table qfbap_dwd.dwd_user_app_pv partition(dt=20190710)
select 
log_id,
user_id,
imei,
log_time,
hour(log_time) log_hour,
visit_os,
os_version,
app_name,
app_version,
device_token,
visit_ip,
province,
city,
current_timestamp() dw_date
from
qfbap_ods.ods_user_app_click_log
where dt=20190710;

--2. load data to user_pc_pv
insert overwrite table qfbap_dwd.dwd_user_pc_pv partition(dt=20190710)
select 
max(log_id),
user_id,
session_id,
cookie_id,
min(visit_time) in_time,
max(visit_time) out_time,
case when min(visit_time) = max(visit_time) then 3 else max(visit_time ) - min(visit_time) end stay_time,
count(1) pv,
visit_os,
browser_name,
visit_ip,
province,
city,
current_timestamp() dw_date
from
qfbap_ods.ods_user_pc_click_log
where dt=20190710
group by
user_id,
session_id,
cookie_id,
visit_os,
browser_name,
visit_ip,
province,
city;
--3. load data to qfbap_dwd.dwd_user
insert overwrite table qfbap_dwd.dwd_user 
select
*
from
qfbap_ods.ods_user;
--4. load data to qfbap_dwd.dwd_user_extend
insert overwrite table qfbap_dwd.dwd_user_extend 
select 
*,
current_timestamp() dw_date
from
qfbap_ods.ods_user_extend;