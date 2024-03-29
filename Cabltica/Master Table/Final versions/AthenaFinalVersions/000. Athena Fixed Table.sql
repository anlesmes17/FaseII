--CREATE TABLE IF NOT EXISTS "lla_cco_int_san"."cr_fixed_table"  AS  

WITH 

Parameters as(
Select 90 as InvoluntaryChurnDays
)

,UsefulFields AS(
SELECT DISTINCT DATE_TRUNC ('Month' , cast(dt as date)) AS month,dt, act_acct_cd, pd_vo_prod_nm, 
PD_TV_PROD_nm, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE, first_value (ACT_ACCT_INST_DT) over(PARTITION  BY act_acct_cd ORDER BY dt ASC) AS MinInst,
first_value (ACT_ACCT_INST_DT) over(PARTITION  BY act_acct_cd ORDER BY ACT_ACCT_INST_DT DESC) AS MaxInst,CST_CHRN_DT AS ChurnDate, DATE_DIFF('DAY',cast(OLDEST_UNPAID_BILL_DT as date), cast(dt as date)) AS MORA, ACT_CONTACT_MAIL_1,act_contact_phone_1,round(FI_VO_MRC_AMT,0) AS mrcVO, round(FI_BB_MRC_AMT,0) AS mrcBB, round(FI_TV_MRC_AMT,0) AS mrcTV,round((FI_VO_MRC_AMT + FI_BB_MRC_AMT + FI_TV_MRC_AMT),0) as avgmrc, round(FI_BILL_AMT_M0,0) AS Bill, ACT_CUST_STRT_DT,lst_pymt_dt,

CASE WHEN pd_vo_prod_nm IS NOT NULL and pd_vo_prod_nm <>'' THEN 1 ELSE 0 END AS RGU_VO,
CASE WHEN pd_tv_prod_nm IS NOT NULL and pd_tv_prod_nm <>'' THEN 1 ELSE 0 END AS RGU_TV,
CASE WHEN pd_bb_prod_nm IS NOT NULL and pd_bb_prod_nm <>'' THEN 1 ELSE 0 END AS RGU_BB,

CASE 
WHEN PD_VO_PROD_nm IS NOT NULL and pd_vo_prod_nm <>'' AND PD_BB_PROD_nm IS NOT NULL and pd_bb_prod_nm<>''
AND PD_TV_PROD_nm IS NOT NULL and pd_tv_prod_nm <>'' THEN '3P'

WHEN (PD_VO_PROD_nm IS NULL or pd_vo_prod_nm ='')  AND PD_BB_PROD_nm IS NOT NULL and pd_bb_prod_nm <>''
AND PD_TV_PROD_nm IS NOT NULL and pd_tv_prod_nm <>'' THEN '2P'

WHEN PD_VO_PROD_nm IS NOT NULL and pd_vo_prod_nm <>'' AND (PD_BB_PROD_nm IS NULL or pd_bb_prod_nm ='') 
AND PD_TV_PROD_nm IS NOT NULL and pd_tv_prod_nm <>'' THEN '2P'

WHEN PD_VO_PROD_nm IS NOT NULL and pd_vo_prod_nm <>'' AND PD_BB_PROD_nm IS NOT NULL and pd_bb_prod_nm <>''
AND (PD_TV_PROD_nm IS NULL or pd_tv_prod_nm ='') THEN '2P'
WHEN PD_VO_PROD_nm IS NULL AND PD_BB_PROD_nm IS NULL AND PD_TV_PROD_nm IS NULL THEN '0P'

ELSE '1P' END AS MIX, pd_bb_tech,

CASE 
WHEN pd_bb_prod_nm LIKE '%FTTH%' OR (pd_tv_prod_nm='NextGen TV' and pd_bb_prod_nm is null) THEN 'FTTH'
ELSE 'HFC' END AS TechFlag,
first_value(fi_outst_age) over(partition by act_acct_cd,date_trunc('month',date(dt)) order by date(dt) desc) as Last_Overdue

FROM "db-analytics-dev"."dna_fixed_cr"
Where (act_cust_typ='RESIDENCIAL' or act_cust_typ='PROGRAMA HOGARES CONECTADOS') and (act_acct_stat='ACTIVO' or act_acct_stat='SUSPENDIDO')
)

,mora_error as(
select distinct month,dt,act_acct_cd,maxinst,mora,prev_mora,next_mora,lst_pymt_dt,
case when ( (mora-prev_mora)>2 and (mora-next_mora)>2 ) or ( (mora-prev_mora)<-2 and (mora-next_mora)<-2 ) then 1 else 0 end as mora_salto
from(
select distinct month, dt, act_acct_cd,FI_OUTST_AGE as mora, maxinst,lst_pymt_dt
,lag(fi_outst_age) over(partition by act_acct_cd order by dt desc) as next_mora
,lag(fi_outst_age) over(partition by act_acct_cd order by dt) as prev_mora
FROM UsefulFields
--group by 1,2,3,4,5,6
))

,mora_arreglada as(
select distinct *
,case when mora_salto=1 then prev_mora+1 
when mora is null and next_mora=prev_mora+2 then prev_mora+1
when prev_mora is null and next_mora is null then null
else mora end as mora_fix
from mora_error
--order by 3,2
)

,productos_churners as(
select *,sum(coalesce(vo,0)+coalesce(bb,0)+coalesce(tv,0)) as invol_rgu_churn
From(select distinct date_trunc('Month',date(dt)) as rgu_month,act_acct_cd,
case when min(pd_vo_prod_nm) is not null then 1 end as vo,
case when min(pd_bb_prod_nm) is not null then 1 end as bb,
case when min(pd_tv_prod_nm) is not null then 1 end as tv
from usefulfields
group by 1,2
)group by 1,2,3,4,5)


,mora_fix_join as(
select a.*,invol_rgu_churn from mora_arreglada a left join productos_churners b on a.act_acct_cd=b.act_acct_cd and rgu_Month=date_trunc('Month',date(a.dt))
order by act_acct_cd,dt
)

,mora_useful_fields as(
select a.*,mora_fix from usefulfields a left join mora_arreglada b on a.dt=b.dt and a.act_acct_cd=b.act_acct_cd
)


,CustomerBase_BOM AS(
SELECT DISTINCT DATE_TRUNC('Month', CAST(dt AS DATE)) AS Month, act_acct_cd AS AccountBOM,dt AS b_date,act_contact_phone_1 as b_phone,
pd_vo_prod_nm as b_vo_nm, pd_tv_prod_nm AS b_tv_nm, pd_bb_prod_nm as b_bb_nm, 
RGU_VO as b_rgu_vo, RGU_TV as b_rgu_tv, RGU_BB AS b_rgu_bb, fi_outst_age as b_overdue, C_CUST_AGE as b_tenure, MinInst as b_min_inst,MaxInst as b_max_inst, MIX AS b_mix,
(coalesce(RGU_VO,0) + coalesce(RGU_TV,0) + coalesce(RGU_BB,0)) AS b_num_rgus, TechFlag as b_tech_flag, MORA AS b_mora, avgmrc as b_avg_mrc,
    BILL AS b_bill_amt,ACT_CUST_STRT_DT AS b_act_cust_strt_dt,
CASE 
WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) THEN 'VO'
WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV'
WHEN (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB'
WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV+VO'
WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV'
WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB+VO'
WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV+VO' END AS b_bundle_name--,
/*
CASE WHEN RGU_BB= 1 THEN act_acct_cd ELSE NULL END As BB_RGU_BOM,
CASE WHEN RGU_TV= 1 THEN act_acct_cd ELSE NULL END As TV_RGU_BOM,
CASE WHEN RGU_VO= 1 THEN act_acct_cd ELSE NULL END As VO_RGU_BOM
*/
    FROM mora_useful_fields c 
    WHERE ((date(dt) = DATE_TRUNC('Month', date(dt)) and DATE_TRUNC('Month', date(dt))<>date('2022-10-01')) OR dt='2022-10-01' OR dt='2022-12-02') and (mora_fix<= 90 or mora_fix is null)
)


,CustomerBase_EOM AS(
SELECT DISTINCT DATE_TRUNC('month', DATE_add('month', -1, cast(dt as date))) AS Month, dt as e_date, act_acct_cd as AccountEOM, act_contact_phone_1 as e_phone, pd_vo_prod_nm as e_vo_nm, 
    pd_tv_prod_nm as e_tv_nm, pd_bb_prod_nm as e_bb_nm, RGU_VO as e_rgu_vo, RGU_TV as e_rgu_tv, RGU_BB AS e_rgu_bb, fi_outst_age as e_overdue, 
    TechFlag as e_tech_flag, C_CUST_AGE as e_tenure, MinInst as e_min_inst,MaxInst as e_max_inst, MIX AS e_mix,
    (coalesce(RGU_VO,0) + coalesce(RGU_TV,0) + coalesce(RGU_BB,0)) AS e_num_rgus, MORA AS e_mora, avgmrc as e_avg_mrc, BILL AS e_bill_amt,ACT_CUST_STRT_DT AS e_act_cust_strt_dt,

    CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) THEN 'VO'
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV'
    WHEN (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV+VO'
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV'
    WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB+VO'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV+VO' END AS e_bundle_name--,

/*
     CASE WHEN RGU_BB= 1 THEN act_acct_cd ELSE NULL END As BB_RGU_EOM,
    CASE WHEN RGU_TV= 1 THEN act_acct_cd ELSE NULL END As TV_RGU_EOM,
    CASE WHEN RGU_VO= 1 THEN act_acct_cd ELSE NULL END As VO_RGU_EOM
*/  
    FROM mora_useful_fields c 
    WHERE ((date(dt) = DATE_TRUNC('Month', date(dt)) and DATE_TRUNC('Month', date(dt))<>date('2022-10-01')) OR dt='2022-10-01' OR dt='2022-12-02') and (mora_fix< 90 or mora_fix is null)
)

,FixedCustomerBase AS(
    SELECT DISTINCT
    CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
   END AS fixed_month,
     CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
  END AS fixed_account,
   CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS active_bom,
   CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS active_eom,
   b_phone,b_date, b_vo_nm, b_tv_nm, b_bb_nm, b_rgu_vo, b_rgu_tv, b_rgu_bb, b_num_rgus, b_overdue, b_tenure, b_min_inst,b_max_inst, b_bundle_name,b_mix, b_tech_flag, b_mora, b_avg_mrc, b_bill_amt,b_act_cust_strt_dt,/*BB_RGU_BOM,TV_RGU_BOM,VO_RGU_BOM,*/
   e_phone,e_date, e_vo_nm, e_tv_nm, e_bb_nm, e_rgu_vo, e_rgu_tv, e_rgu_bb, e_num_rgus, e_overdue, e_tenure, e_min_inst,e_max_inst, e_bundle_name,e_mix, e_tech_flag, e_mora, e_avg_mrc, e_bill_amt,e_act_cust_strt_dt/*,BB_RGU_EOM,TV_RGU_EOM,VO_RGU_EOM*/
  FROM CustomerBase_BOM b FULL OUTER JOIN CustomerBase_EOM e ON b.AccountBOM = e.AccountEOM AND b.Month = e.Month
)

,ServiceOrders AS (
    SELECT * FROM "db-stage-dev"."so_cr" 
)


--------------------------------------Main Movements------------------------------------------
,MAINMOVEMENTBASE AS(
 SELECT f.*, CASE
 WHEN (e_num_rgus - b_num_rgus)=0 THEN '01. Same RGUs'
 WHEN (e_num_rgus - b_num_rgus)>0 THEN '02. Upsell'
 WHEN (e_num_rgus - b_num_rgus)<0 then '03. Downsell'
 WHEN (b_num_rgus IS NULL AND e_num_rgus > 0 AND DATE_TRUNC ('MONTH', e_act_cust_strt_dt) <> Fixed_Month) 
 AND date_diff('month',e_max_inst,cast(Fixed_Month as timestamp))<=1
 THEN '04. Come Back to Life'
 WHEN (b_num_rgus IS NULL AND e_num_rgus > 0 AND date_diff('month',e_act_cust_strt_dt,cast(Fixed_Month as timestamp))<=1)
 THEN '05. New Customer'
 WHEN active_bom = 1 AND active_eom = 0 THEN '06. Loss'
 WHEN (b_num_rgus IS NULL AND e_num_rgus > 0 AND DATE_TRUNC ('MONTH', e_act_cust_strt_dt) <> Fixed_Month) Then '07. Missing Customer'
 END AS main_movement, e_num_rgus - b_num_rgus as dif_total_rgu
 FROM FixedCustomerBase f
)


,spin_movementBASE AS (
    SELECT b.*,
    CASE
    WHEN b_tenure <=6 THEN 'Early Tenure'
    WHEN (b_tenure >6 and b_tenure <= 12)  THEN 'Mid Tenure'
    when b_tenure > 12 then 'Late Tenure'
    ELSE NULL END AS b_fixed_tenure_segment,
    
    CASE
    WHEN e_tenure <=6 THEN 'Early Tenure'
    WHEN (e_tenure >6 and e_tenure <= 12)  THEN 'Mid Tenure'
    WHEN e_tenure > 12 then 'Late Tenure'
    ELSE NULL END AS e_fixed_tenure_segment,
    
    
    CASE 
    WHEN main_movement='Same RGUs' AND (e_bill_amt - b_bill_amt) > 0 THEN '1. Up-spin' 
    WHEN main_movement='Same RGUs' AND (e_bill_amt - b_bill_amt) < 0 THEN '2. Down-spin' 
    ELSE '3. No Spin' END AS spin_movement
    FROM MAINMOVEMENTBASE b
)



--------------------------------------- Fixed Churn Flags --------------------------------------------------------
--------------------------------------------Voluntary

,InactiveUsers as(
Select distinct Fixed_Month,Fixed_Account,case
when fixed_account is not null THEN '1. Fixed Voluntary Churner'
Else Null End as VolChurners
From spin_movementBASE
WHERE active_bom=1 and (active_eom=0 or active_eom is null) 
)

,max_rgus as( 
select distinct act_acct_cd,sum(vo+bb+tv) as max_rgus_count from(
select distinct act_acct_cd,
case when first_value (pd_vo_prod_nm) over(PARTITION  BY act_acct_cd ORDER BY dt) is not null then 1 else 0 end as vo,
case when first_value (pd_bb_prod_nm) over(PARTITION  BY act_acct_cd ORDER BY dt) is not null then 1 else 0 end as bb,
case when first_value (pd_tv_prod_nm) over(PARTITION  BY act_acct_cd ORDER BY dt) is not null then 1 else 0 end as tv
FROM usefulfields
) group by 1
)

,max_rgus_inactive_users as(
select a.*,max_rgus_count from InactiveUsers a left join max_rgus b on fixed_account=act_acct_cd
)

/*
,Deinstallations as(
Select distinct date_trunc('Month',order_start_date) as D_Month, account_name From "db-stage-dev"."so_cr"
WHERE order_type = 'DESINSTALACION' AND (order_status <> 'CANCELADA' OR order_status <> 'ANULADA') 
)

,ChurnDeinstallations as(
select f.*,b.*, case
when Account_Name is not null THEN '1. Fixed Voluntary Churner'
Else Null End as VolChurners
From InactiveUsers f inner join Deinstallations b 
ON account_name=Fixed_Account and date_diff('Month',D_Month,Fixed_month) <=1)
*/

--------------------------------------------Involunary

,FIRSTCUSTRECORD AS (
    SELECT DATE_TRUNC('MONTH',date(dt)) AS MES, act_acct_cd AS Account, min(date(dt)) AS FirstCustRecord
    FROM mora_arreglada
    WHERE CAST(mora_fix as INT) < (select InvoluntaryChurnDays From parameters)
    --WHERE date(dt) = date_trunc('MONTH', DATE(dt)) + interval '1' MONTH - interval '1' day
    Group by 1,2
)

,LastCustRecord as(
    SELECT  DATE_TRUNC('MONTH', DATE(dt)) AS MES, act_acct_cd AS Account, max(date(dt)) as LastCustRecord
    FROM mora_arreglada
      --WHERE DATE(LOAD_dt) = date_trunc('MONTH', DATE(LOAD_dt)) + interval '1' MONTH - interval '1' day
   Group by 1,2
   --order by 1,2
)

 ,NO_OVERDUE AS(
 SELECT DISTINCT DATE_TRUNC('MONTH',date(dt)) AS MES, act_acct_cd AS Account, mora_fix
 FROM mora_arreglada t
 INNER JOIN FIRSTCUSTRECORD  r ON r.account = t.act_acct_cd
 WHERE CAST(mora_fix as INT) < (select InvoluntaryChurnDays From parameters)
 GROUP BY 1, 2, 3
)


 ,OVERDUELASTDAY AS(
 SELECT DISTINCT DATE_TRUNC('MONTH', DATE(dt)) AS MES, act_acct_cd AS Account, mora_fix,invol_rgu_churn,
 (date_diff('DAY',MaxInst,DATE(dt))) as ChurnTenureDays
 FROM mora_fix_join t
 INNER JOIN LastCustRecord r ON date(t.dt) = r.LastCustRecord and 
 r.account = t.act_acct_cd
 WHERE date(t.dt)=r.LastCustRecord and CAST(mora_fix AS INTEGER) >= (select InvoluntaryChurnDays From parameters)
 GROUP BY 1,2,3,4,5
 )
 
 
 ,INVOLUNTARYNETCHURNERS AS(
 SELECT DISTINCT n.MES AS Month, n. account, l.ChurnTenureDays,invol_rgu_churn
 FROM NO_OVERDUE n INNER JOIN OVERDUELASTDAY l ON n.account = l.account and n.MES = l.MES
 )

,InvoluntaryChurners AS(
SELECT DISTINCT i.Month, i.Account AS ChurnAccount, i.ChurnTenureDays,invol_rgu_churn
,CASE WHEN i.Account IS NOT NULL THEN '2. Fixed Involuntary Churner' END AS InvolChurner
FROM INVOLUNTARYNETCHURNERS i left join usefulfields f on i.account=f.act_acct_cd and i.month=date_trunc('month',date(f.dt))
where last_overdue>=(select InvoluntaryChurnDays From parameters)
GROUP BY 1, Account,4, ChurnTenureDays
)

,FinalInvoluntaryChurners AS(
    SELECT DISTINCT MONTH, ChurnAccount,InvolChurner,invol_rgu_churn
    FROM InvoluntaryChurners
    WHERE InvolChurner = '2. Fixed Involuntary Churner'
)

,AllChurners AS(
SELECT f.*,b.* From max_rgus_inactive_users f Full Outer Join FinalInvoluntaryChurners b
ON Fixed_month=Month and ChurnAccount=Fixed_Account
)
,FinalFixedChurners as(
select 
case when Month is not null THEN Month else fixed_Month End as ChurnMonth,
case when ChurnAccount is not null THEN ChurnAccount else Fixed_Account End as Churn_Account,
case 
when InvolChurner is not null and VolChurners is not null then InvolChurner
when InvolChurner is not null then InvolChurner
when VolChurners  is not null then VolChurners 
end as fixed_churner_type,
invol_rgu_churn,max_rgus_count
From AllChurners
)

,ChurnersFixedTable as(
select f.*, case when active_eom=1 then null else fixed_churner_type end as fixed_churner_type,
invol_rgu_churn,max_rgus_count FROM spin_movementBASE f left join FinalFixedChurners b
on Fixed_Month=ChurnMonth and Fixed_Account=Churn_Account
)
--------------------------------------------------------------------------- Rejoiners -------------------------------------------------------------

,Inactive_Users as(
Select Distinct Fixed_Month as exit_month, fixed_account as exit_account,fixed_churner_type,date_add('month', 1, Fixed_Month) AS rejoiner_month
From ChurnersFixedTable
Where fixed_churner_type is not null
)

,mora_inactive_users as(
select distinct month,mora_fix,act_acct_cd From mora_arreglada
Where mora_fix <=(select InvoluntaryChurnDays From parameters) or (mora_fix is null and date_trunc('Month',lst_pymt_dt)=month)
--order by 1,2
)

,Rejoiners as(
Select rejoiner_month,act_acct_cd as fixed_rejoiner,case 
when fixed_churner_type='1. Fixed Voluntary Churner' THEN '1. Fixed Voluntary Rejoiner'
when fixed_churner_type='2. Fixed Involuntary Churner' THEN '2. Fixed Involuntary Rejoiner'
Else null end as fixed_rejoiner_type 
From Inactive_Users a inner join mora_inactive_users b
ON exit_account=act_acct_cd and rejoiner_month=month
--date_diff('month',exit_month,month)<=2 and date_diff('month',exit_month,month)>0
)

,rejoiners_master_table as(
Select distinct a.*,fixed_rejoiner,fixed_rejoiner_type From ChurnersFixedTable a left join rejoiners 
ON rejoiner_month=fixed_month and fixed_rejoiner=fixed_account
)

,FinalTable as(
SELECT *,CASE
WHEN fixed_churner_type='2. Fixed Involuntary Churner' then invol_rgu_churn
WHEN fixed_churner_type='1. Fixed Voluntary Churner' THEN max_rgus_count
WHEN main_movement='03. Downsell' THEN (b_num_rgus - e_num_rgus)
ELSE 0 END AS fixed_rgu_churn,

CONCAT(coalesce(b_vo_nm,'-'),coalesce(b_tv_nm,'-'),coalesce(b_bb_nm,'-')) AS b_plan
,CONCAT(coalesce(e_vo_nm,'-'),coalesce(e_tv_nm,'-'),coalesce(e_bb_nm,'-')) AS e_plan
FROM rejoiners_master_table
)
--/*
Select * From FinalTable
where Fixed_Month=date('2022-10-01') and 
where fixed_account='1157408'
order by fixed_month
*/
--/*
select distinct fixed_month,fixed_churner_type,count(distinct fixed_account) from FinalTable
where rgu_churn is not null 
group by 1,2
--*/

/*
,with_rgus as(
select distinct month,dt as date_with_rgus,act_acct_cd,pd_vo_prod_nm,pd_bb_prod_nm,pd_tv_prod_nm from usefulfields
where pd_vo_prod_nm is not null or pd_bb_prod_nm is not null or pd_tv_prod_nm is not null --and month=date('2022-09-01')
)

,without_rgus as(
select distinct month as without_month,dt,act_acct_cd as without_account,pd_vo_prod_nm,pd_bb_prod_nm,pd_tv_prod_nm from usefulfields
where pd_vo_prod_nm is null and pd_bb_prod_nm is null and pd_tv_prod_nm is null and month=date('2022-10-01') and dt=('2022-10-05')
)

,intermitent_rgus as (
select a.*,b.* from with_rgus a inner join without_rgus b on a.act_acct_cd=b.without_account and date_add('Month',1,a.month)=b.without_month
)

,intermitent_users as (
select a.*,b.* from intermitent_rgus a inner join with_rgus b on a.act_acct_cd=b.act_acct_cd and a.month=date_add('Month',-1,b.month)
and date(b.date_with_rgus)>date('2022-10-05')
)
--select distinct without_month, without_account as act_acct_cd from intermitent_users
--where without_month=date('2022-11-01')

select distinct without_month,count(distinct without_account) from intermitent_users
group by 1
*/


/*
select distinct fixed_month,fixed_account from finaltable
WHERE (fixed_churner_type='1. Fixed Voluntary Churner' or main_movement='03. Downsell') and (rgu_churn=0 or rgu_churn is null) and fixed_month=date('2022-10-01')



Select distinct Fixed_Month,fixed_rejoiner_type,count(distinct fixed_account) From FinalTable
--WHERE rgu_churn IS NOT NULL
group by 1,2
order by 1,2


,Oct_Churners as(
select * From FinalTable
where fixed_month=date('2022-10-01') and fixed_churner_type='2. Fixed Involuntary Churner'
)

,Nov120 as(
select distinct month,act_acct_cd
From mora_arreglada
Where mora_fix=119 and month=date('2022-11-01')
)
*/

/*
Select distinct Fixed_Month,fixed_rejoiner_type,count(distinct fixed_account)
From FinalTable
group by 1,2
order by 1,2
*/

/*
,gap_users as(
Select distinct fixed_month,fixed_account,month,act_acct_cd From oct_churners left join Nov120 
ON date_add('Month',1,fixed_month)=month and fixed_account=act_acct_cd
--where month=date('2022-11-01')
)

,gap_table as(
select a.Fixed_Month as Month,a.Fixed_Account as act_acct_cd,b_vo_nm as pd_vo_prod_nm,b_tv_nm as pd_tv_prod_nm,b_bb_nm as pd_bb_prod_nm From FinalTable a inner join gap_users b 
ON a.Fixed_Month=b.month and a.Fixed_Account=b.act_acct_cd
)
*/
/*
select distinct fixed_month,count (distinct fixed_account),count (distinct act_acct_cd)
From gap_users
group by 1
order by 1
*/

/*
Select distinct Fixed_Month,fixed_churner_type,count(distinct fixed_account) From FinalTable
WHERE rgu_churn IS NOT NULL
group by 1,2
order by 1,2
*/

/*
select distinct Fixed_Month,b_mix,count(fixed_account) as NumUsers,sum(b_num_rgus) as NumRGUs
From FinalTable
WHERE fixed_churner_type='2. Fixed Involuntary Churner'
group by 1,2
order by 1,2
*/
