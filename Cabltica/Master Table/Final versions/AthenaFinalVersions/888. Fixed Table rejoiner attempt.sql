--CREATE TABLE IF NOT EXISTS "lla_cco_int_san"."cr_fixed_table"  AS  

WITH 

Parameters as(
Select 90 as InvoluntaryChurnDays
)

,UsefulFields AS(
SELECT DISTINCT DATE_TRUNC ('Month' , cast(dt as date)) AS Month,dt, act_acct_cd, pd_vo_prod_nm, 
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


,CustomerBase_BOM AS(
SELECT DISTINCT DATE_TRUNC('Month', CAST(dt AS DATE)) AS Month, act_acct_cd AS AccountBOM,dt AS B_DATE,act_contact_phone_1 as B_Phone,
pd_vo_prod_nm as B_VO_nm, pd_tv_prod_nm AS B_TV_nm, pd_bb_prod_nm as B_BB_nm, 
RGU_VO as B_RGU_VO, RGU_TV as B_RGU_TV, RGU_BB AS B_RGU_BB, fi_outst_age as B_Overdue, C_CUST_AGE as B_Tenure, MinInst as B_MinInst,MaxInst as B_Maxinst, MIX AS B_MIX,
(RGU_VO + RGU_TV + RGU_BB) AS B_NumRGUs, TechFlag as B_TechFlag, MORA AS B_MORA, 

mrcVO as B_VO_MRC, mrcBB as B_BB_MRC, mrcTV as B_TV_MRC, avgmrc as B_AVG_MRC,
    BILL AS B_BILL_AMT,ACT_CUST_STRT_DT AS B_ACT_CUST_STRT_DT,
CASE 
WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) THEN 'VO'
WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV'
WHEN (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB'
WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV+VO'
WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV'
WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB+VO'
WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV+VO' END AS B_BundleName,

CASE WHEN RGU_BB= 1 THEN act_acct_cd ELSE NULL END As BB_RGU_BOM,
CASE WHEN RGU_TV= 1 THEN act_acct_cd ELSE NULL END As TV_RGU_BOM,
CASE WHEN RGU_VO= 1 THEN act_acct_cd ELSE NULL END As VO_RGU_BOM
    
    FROM UsefulFields c 
    WHERE (date(dt) = DATE_TRUNC('Month', date(dt)) and DATE_TRUNC('Month', date(dt))<>date('2022-10-01')) OR dt='2022-10-07' 
)


,CustomerBase_EOM AS(
SELECT DISTINCT DATE_TRUNC('month', DATE_add('month', -1, cast(dt as date))) AS Month, dt as E_Date, act_acct_cd as AccountEOM, act_contact_phone_1 as E_Phone, pd_vo_prod_nm as E_VO_nm, 
    pd_tv_prod_nm as E_TV_nm, pd_bb_prod_nm as E_BB_nm, RGU_VO as E_RGU_VO, RGU_TV as E_RGU_TV, RGU_BB AS E_RGU_BB, fi_outst_age as E_Overdue, 
    TechFlag as E_TechFlag, C_CUST_AGE as E_Tenure, MinInst as E_MinInst,MaxInst as E_MaxInst, MIX AS E_MIX,
    (RGU_VO + RGU_TV + RGU_BB) AS E_NumRGUs, MORA AS E_MORA, mrcVO AS E_VO_MRC, mrcBB as E_BB_MRC, mrcTV as E_TV_MRC, avgmrc as E_AVG_MRC, BILL AS E_BILL_AMT,ACT_CUST_STRT_DT AS E_ACT_CUST_STRT_DT,

    CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) THEN 'VO'
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV'
    WHEN (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) THEN 'TV+VO'
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV'
    WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN 'BB+VO'
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN 'BB+TV+VO' END AS E_BundleName,
     CASE WHEN RGU_BB= 1 THEN act_acct_cd ELSE NULL END As BB_RGU_EOM,
    CASE WHEN RGU_TV= 1 THEN act_acct_cd ELSE NULL END As TV_RGU_EOM,
    CASE WHEN RGU_VO= 1 THEN act_acct_cd ELSE NULL END As VO_RGU_EOM
    
    FROM UsefulFields c 
    WHERE (date(dt) = DATE_TRUNC('Month', date(dt)) and DATE_TRUNC('Month', date(dt))<>date('2022-10-01')) OR dt='2022-10-07'
    --and fi_outst_age <=90
)

,FixedCustomerBase AS(
    SELECT DISTINCT
    CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
   END AS Fixed_Month,
     CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
  END AS Fixed_Account,
   CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM,
   CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM,
   
   B_Phone,B_Date, B_VO_nm, B_TV_nm, B_BB_nm, B_RGU_VO, B_RGU_TV, B_RGU_BB, B_NumRGUs, B_Overdue, B_Tenure, B_MinInst,B_Maxinst, B_BundleName,B_MIX, B_TechFlag, B_MORA, B_VO_MRC, B_BB_MRC, B_TV_MRC, B_AVG_MRC, B_BILL_AMT,B_ACT_CUST_STRT_DT,BB_RGU_BOM,TV_RGU_BOM,VO_RGU_BOM,
   E_phone,E_Date, E_VO_nm, E_TV_nm, E_BB_nm, E_RGU_VO, E_RGU_TV, E_RGU_BB, E_NumRGUs, E_Overdue, E_Tenure, E_MinInst,E_Maxinst, E_BundleName,E_MIX, E_TechFlag, E_MORA, E_VO_MRC, E_BB_MRC, E_TV_MRC, E_AVG_MRC, E_BILL_AMT,E_ACT_CUST_STRT_DT,BB_RGU_EOM,TV_RGU_EOM,VO_RGU_EOM
  FROM CustomerBase_BOM b FULL OUTER JOIN CustomerBase_EOM e ON b.AccountBOM = e.AccountEOM AND b.Month = e.Month
)

,ServiceOrders AS (
    SELECT * FROM "db-stage-dev"."so_cr" 
)


--------------------------------------Main Movements------------------------------------------
,MAINMOVEMENTBASE AS(
 SELECT f.*, CASE
 WHEN (E_NumRGUs - B_NumRGUs)=0 THEN '01. Same RGUs'
 WHEN (E_NumRGUs - B_NumRGUs)>0 THEN '02. Upsell'
 WHEN (E_NumRGUs - B_NumRGUs)<0 then '03. Downsell'
 WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC ('MONTH', E_ACT_CUST_STRT_DT) <> Fixed_Month) 
 AND date_diff('month',E_MaxInst,cast(Fixed_Month as timestamp))<=1
 THEN '04. Come Back to Life'
 WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND date_diff('month',E_ACT_CUST_STRT_DT,cast(Fixed_Month as timestamp))<=1)
 THEN '05. New Customer'
 WHEN ActiveBOM = 1 AND ActiveEOM = 0 THEN '06. Loss'
 WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC ('MONTH', E_ACT_CUST_STRT_DT) <> Fixed_Month) Then '07. Missing Customer'
 END AS MainMovement,
 E_RGU_BB - B_RGU_BB as DIF_RGU_BB , E_RGU_TV - B_RGU_TV as DIF_RGU_TV , E_RGU_VO - B_RGU_VO as DIF_RGU_VO , E_NumRGUs - B_NumRGUs as DIF_TOTAL_RGU
 FROM FixedCustomerBase f
)


,SPINMOVEMENTBASE AS (
    SELECT b.*,
    CASE
    WHEN B_Tenure <=6 THEN 'Early Tenure'
    WHEN (B_Tenure >6 and b_tenure <= 12)  THEN 'Mid Tenure'
    when b_tenure > 12 then 'Late Tenure'
    ELSE NULL END AS B_FixedTenureSegment,
    
    CASE
    WHEN E_Tenure <=6 THEN 'Early Tenure'
    WHEN (E_Tenure >6 and e_tenure <= 12)  THEN 'Mid Tenure'
    WHEN e_tenure > 12 then 'Late Tenure'
    ELSE NULL END AS E_FixedTenureSegment,
    
    
    CASE 
    WHEN MainMovement='Same RGUs' AND (E_BILL_AMT - B_BILL_AMT) > 0 THEN '1. Up-spin' 
    WHEN MainMovement='Same RGUs' AND (E_BILL_AMT - B_BILL_AMT) < 0 THEN '2. Down-spin' 
    ELSE '3. No Spin' END AS SpinMovement
    FROM MAINMOVEMENTBASE b
)



--------------------------------------- Fixed Churn Flags --------------------------------------------------------
--------------------------------------------Voluntary

,InactiveUsers as(
Select distinct Fixed_Month,Fixed_Account
From SPINMOVEMENTBASE
WHERE ActiveBOM=1 and (ActiveEOM=0 or ActiveEOM is null) 
)

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

--------------------------------------------Involunary

,mora_error as(
select distinct month,dt,act_acct_cd,maxinst,mora,prev_mora,next_mora,lst_pymt_dt,
case when ( (mora-prev_mora)>2 and (mora-next_mora)>2 ) or ( (mora-prev_mora)<-2 and (mora-next_mora)<-2 ) then 1 else 0 end as mora_salto
from(
select distinct month, dt, act_acct_cd,FI_OUTST_AGE as mora, maxinst,lst_pymt_dt
,lag(fi_outst_age) over(partition by act_acct_cd order by dt desc) as next_mora
,lag(fi_outst_age) over(partition by act_acct_cd order by dt) as prev_mora
FROM UsefulFields 
)
--order by act_acct_cd,dt
)
,mora_arreglada as(
select distinct *
,case when mora_salto=1 then prev_mora+1 
when mora is null and next_mora=prev_mora+2 then prev_mora+1
when prev_mora is null and next_mora is null then null
else mora end as mora_fix
from mora_error
--order by 3,2
)
/*
select distinct month,mora_fix,count(distinct act_acct_cd)
From mora_arreglada
where month=date('2022-11-01')
group by 1,2
order by 1,2
*/
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
 SELECT DISTINCT DATE_TRUNC('MONTH', DATE(dt)) AS MES, act_acct_cd AS Account, mora_fix,
 (date_diff('DAY',MaxInst,DATE(dt))) as ChurnTenureDays
 FROM mora_arreglada t
 INNER JOIN LastCustRecord r ON date(t.dt) = r.LastCustRecord and 
 r.account = t.act_acct_cd
 WHERE date(t.dt)=r.LastCustRecord and CAST(mora_fix AS INTEGER) >= (select InvoluntaryChurnDays From parameters)
 GROUP BY 1, 2, 3, 4
 )
 
 ,INVOLUNTARYNETCHURNERS AS(
 SELECT DISTINCT n.MES AS Month, n. account, l.ChurnTenureDays
 FROM NO_OVERDUE n INNER JOIN OVERDUELASTDAY l ON n.account = l.account and n.MES = l.MES
 )

,InvoluntaryChurners AS(
SELECT DISTINCT i.Month, i.Account AS ChurnAccount, i.ChurnTenureDays
,CASE WHEN i.Account IS NOT NULL THEN '2. Fixed Involuntary Churner' END AS InvolChurner
FROM INVOLUNTARYNETCHURNERS i left join usefulfields f on i.account=f.act_acct_cd and i.month=date_trunc('month',date(f.dt))
where last_overdue>=(select InvoluntaryChurnDays From parameters)
GROUP BY 1, Account,4, ChurnTenureDays
)

,FinalInvoluntaryChurners AS(
    SELECT DISTINCT MONTH, ChurnAccount, InvolChurner
    FROM InvoluntaryChurners
    WHERE InvolChurner = '2. Fixed Involuntary Churner'
)

,AllChurners AS(
SELECT f.*,b.* From ChurnDeinstallations f Full Outer Join FinalInvoluntaryChurners b
ON D_Month=Month and ChurnAccount=Account_Name
)
,FinalFixedChurners as(
select 
case when Fixed_Month is not null THEN Fixed_Month else Month End as ChurnMonth,
case when Fixed_Account is not null THEN Fixed_Account else ChurnAccount End as Churn_Account,
case when VolChurners is not null THEN VolChurners else InvolChurner end as FixedChurnerType
From AllChurners
)

,ChurnersFixedTable as(
select f.*,FixedChurnerType FROM SPINMOVEMENTBASE f left join FinalFixedChurners b
on Fixed_Month=ChurnMonth and Fixed_Account=Churn_Account
)
/*
select * From churnersfixedtable
where fixed_account='1038583'
order by fixed_month
*/
--------------------------------------------------------------------------- Rejoiners -------------------------------------------------------------

/*
,InactiveUsersMonth AS (
SELECT DISTINCT Fixed_Month AS ExitMonth, Fixed_Account,DATE_ADD('MONTH', 1, Fixed_Month) AS RejoinerMonth 
FROM FixedCustomerBase 
WHERE ActiveBOM=1 AND ActiveEOM=0
)
--------------------Revisar la lógica de RejoinersPopulation y de FixedRejoinerFebPopulation. Creo que seguramente ya no hace falta el segundo---------
,RejoinersPopulation AS(
SELECT f.*,RejoinerMonth
,CASE WHEN i.Fixed_Account IS NOT NULL THEN 1 ELSE 0 END AS RejoinerPopFlag
------ Variabilizar
,CASE WHEN RejoinerMonth>=date('2022-10-01') AND RejoinerMonth<=DATE_ADD('MONTH', 1, date('2022-10-01') ) THEN 1 ELSE 0 END AS Fixed_PR
FROM FixedCustomerBase f LEFT JOIN InactiveUsersMonth i ON f.Fixed_Account=i.Fixed_Account AND Fixed_Month=ExitMonth
)
,FixedRejoinerFebPopulation AS(
SELECT DISTINCT Fixed_Month,RejoinerPopFlag,Fixed_PR,Fixed_Account,date('2022-10-01') AS Month
FROM RejoinersPopulation
WHERE RejoinerPopFlag=1
AND Fixed_PR=1
AND Fixed_Month<>date('2022-10-01')
GROUP BY 1,2,3,4
)
,FullFixedBase_Rejoiners AS(
SELECT DISTINCT f.*,Fixed_PR
,CASE WHEN Fixed_PR=1 AND MainMovement='04. Come Back to Life'
THEN 1 ELSE 0 END AS Fixed_Rejoiner
FROM ChurnersFixedTable f LEFT JOIN FixedRejoinerFebPopulation r ON f.Fixed_Account=r.Fixed_Account AND f.Fixed_Month=CAST(r.Month AS DATE)
)
*/

,Inactive_Users as(
Select Distinct Fixed_Month as exit_month, fixed_account as exit_account,FixedChurnerType,date_add('month', 1, Fixed_Month) AS rejoiner_month
From ChurnersFixedTable
Where FixedChurnerType is not null
)

,mora_inactive_users as(
select distinct month,mora_fix,act_acct_cd From mora_arreglada
Where mora_fix <=(select InvoluntaryChurnDays From parameters) or (mora_fix is null and date_trunc('Month',lst_pymt_dt)=month)
--order by 1,2
)

,Rejoiners as(
Select rejoiner_month,act_acct_cd as fixed_rejoiner,case 
when FixedChurnerType='1. Fixed Voluntary Churner' THEN '1. Fixed Voluntary Rejoiner'
when FixedChurnerType='2. Fixed Involuntary Churner' THEN '2. Fixed Involuntary Rejoiner'
Else null end as rejoiner_type 
From Inactive_Users a inner join mora_inactive_users b
ON exit_account=act_acct_cd and rejoiner_month=month
--date_diff('month',exit_month,month)<=2 and date_diff('month',exit_month,month)>0
)

,rejoiners_master_table as(
Select distinct a.*,fixed_rejoiner,rejoiner_type From ChurnersFixedTable a left join rejoiners 
ON rejoiner_month=fixed_month and fixed_rejoiner=fixed_account
)

,FinalTable as(
SELECT *,CASE
WHEN FixedChurnerType is not null THEN b_NumRGUs
WHEN MainMovement='03. Downsell' THEN (B_NumRGUs - E_NumRGUs)
ELSE NULL END AS RGU_Churn,

CONCAT(coalesce(B_VO_nm,'-'),coalesce(B_TV_nm,'-'),coalesce(B_BB_nm,'-')) AS B_PLAN
,CONCAT(coalesce(E_VO_nm,'-'),coalesce(E_TV_nm,'-'),coalesce(E_BB_nm,'-')) AS E_PLAN
FROM rejoiners_master_table
)

--select * From FinalTable
/*
select * from finaltable
where fixed_account='830031'
order by fixed_month
*/
,Oct_Churners as(
select * From FinalTable
where fixed_month=date('2022-10-01') and FixedChurnerType='2. Fixed Involuntary Churner'
)

,Nov120 as(
select distinct month,act_acct_cd
From mora_arreglada
Where mora_fix=119 and month=date('2022-11-01')
)
/*
Select distinct Fixed_Month,rejoiner_type,count(distinct fixed_account)
From FinalTable
group by 1,2
order by 1,2
*/
--,gap_users as(
Select distinct fixed_month,fixed_account,month,act_acct_cd From oct_churners left join Nov120 
ON date_add('Month',1,fixed_month)=month and fixed_account=act_acct_cd
--)




/*
select distinct fixed_month,count (distinct fixed_account),count (distinct act_acct_cd)
From gap_users
group by 1
order by 1
*/

/*
Select distinct Fixed_Month,FixedChurnerType,count(distinct fixed_account) From FinalTable
WHERE RGU_Churn IS NOT NULL
group by 1,2
order by 1,2
*/

/*
select distinct Fixed_Month,B_MIX,count(fixed_account) as NumUsers,sum(B_NumRGUs) as NumRGUs
From FinalTable
WHERE FixedChurnerType='2. Fixed Involuntary Churner'
group by 1,2
order by 1,2
*/
