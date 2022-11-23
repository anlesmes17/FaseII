WITH

UsefulFields AS(
SELECT DISTINCT *,date_trunc('Month',date(dt)) as Month FROM "db-analytics-dev"."dna_fixed_cr"
Where (act_cust_typ='RESIDENCIAL' or act_cust_typ='PROGRAMA HOGARES CONECTADOS') and act_acct_stat='ACTIVO'
)

,mora_error as(
select distinct month,dt,act_acct_cd,mora,prev_mora,next_mora
,case when ( (mora-prev_mora)>2 and (mora-next_mora)>2 ) or ( (mora-prev_mora)<-2 and (mora-next_mora)<-2 ) then 1 else 0 end as mora_salto
from(
select distinct month, dt, act_acct_cd,FI_OUTST_AGE as mora
,lag(fi_outst_age) over(partition by act_acct_cd order by dt desc) as next_mora
,lag(fi_outst_age) over(partition by act_acct_cd order by dt) as prev_mora
FROM UsefulFields 
)
order by act_acct_cd,dt
)
,mora_arreglada as(
select distinct *
,case 
when mora_salto=1 then prev_mora+1 
when mora is null and next_mora=prev_mora+2 then prev_mora+1 
else mora end as mora_fix
from mora_error
order by 3,2
)
select distinct Month,Mora_Fix,count(distinct act_acct_cd)
From mora_arreglada
Group by 1,2
Order by 1,2
