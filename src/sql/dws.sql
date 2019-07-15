insert overwrite table qfbap_dws.dws_user_visit_month1 
select 
user_id,
type,
cnt,
content,
row_number() over (distribute by user_id,type sort by cnt desc) rn,
current_timestamp() dw_date
from
(select 
user_id,
'visit_ip' as type,
sum(pv) as cnt,
visit_ip as content
from
qfbap_dwd.dwd_user_pc_pv
group by
user_id,
visit_ip
union
select 
user_id,
'cookie_id' as type,
sum(pv) as cnt,
visit_ip as content
from
qfbap_dwd.dwd_user_pc_pv
group by
user_id,
visit_ip
union
select 
user_id,
'浏览器' as type,
sum(pv) as cnt,
visit_ip as content
from
qfbap_dwd.dwd_user_pc_pv
group by
user_id,
visit_ip
union
select 
user_id,
'visit_os' as type,
sum(pv) as cnt,
visit_ip as content
from 
qfbap_dwd.dwd_user_pc_pv
group by
user_id,
visit_ip)tmp