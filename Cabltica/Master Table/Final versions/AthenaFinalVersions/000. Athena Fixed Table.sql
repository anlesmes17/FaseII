WITH 
UsefulFields AS(
SELECT DISTINCT DATE_TRUNC ('Month' , cast(dt as date)) AS Month,dt, act_acct_cd, pd_vo_prod_nm, 
PD_TV_PROD_nm, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE, first_value (ACT_ACCT_INST_DT) over(PARTITION  BY act_acct_cd ORDER BY dt ASC) AS MinInst,
first_value (ACT_ACCT_INST_DT) over(PARTITION  BY act_acct_cd ORDER BY ACT_ACCT_INST_DT DESC) AS MaxInst,CST_CHRN_DT AS ChurnDate, DATE_DIFF('DAY',cast(OLDEST_UNPAID_BILL_DT as date), cast(dt as date)) AS MORA, ACT_CONTACT_MAIL_1,act_contact_phone_1,round(FI_VO_MRC_AMT,0) AS mrcVO, round(FI_BB_MRC_AMT,0) AS mrcBB, round(FI_TV_MRC_AMT,0) AS mrcTV,round((FI_VO_MRC_AMT + FI_BB_MRC_AMT + FI_TV_MRC_AMT),0) as avgmrc, round(FI_BILL_AMT_M0,0) AS Bill, ACT_CUST_STRT_DT,

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
ELSE '1P' END AS MIX, pd_bb_tech,

CASE 
WHEN pd_bb_prod_nm LIKE '%FTTH%' OR pd_tv_prod_nm ='NextGen TV' THEN 'FTTH'
ELSE 'HFC' END AS TechFlag

FROM "db-analytics-dev"."dna_fixed_cr"
where act_acct_stat='ACTIVO'
)


,CustomerBase_BOM AS(
SELECT DISTINCT DATE_TRUNC('Month', CAST(dt AS DATE)) AS Month, act_acct_cd AS AccountBOM,dt AS B_DATE,act_contact_phone_1 as B_Phone,
pd_vo_prod_nm as B_VO_nm, pd_tv_prod_nm AS B_TV_nm, pd_bb_prod_nm as B_BB_nm, 
RGU_VO as B_RGU_VO, RGU_TV as B_RGU_TV, RGU_BB AS B_RGU_BB, fi_outst_age as B_Overdue, C_CUST_AGE as B_Tenure, MinInst as B_MinInst,MaxInst as B_Maxinst, MIX AS B_MIX,
(RGU_VO + RGU_TV + RGU_BB) AS B_NumRGUs, TechFlag as B_TechFlag, MORA AS B_MORA, 

mrcVO as B_VO_MRC, mrcBB as B_BB_MRC, mrcTV as B_TV_MRC, avgmrc as B_AVG_MRC,
    BILL AS B_BILL_AMT,ACT_CUST_STRT_DT AS B_ACT_CUST_STRT_DT,

--CASE 
--WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) 
--THEN '1P'
--WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) OR (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) 
--THEN '2P'
--WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN '3P' END AS B_Bundle_Type,

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
    
--CASE WHEN (RGU_BB = 1 AND RGU_TV = 0 AND RGU_VO = 0) OR  (RGU_BB = 0 AND RGU_TV = 1 AND RGU_VO = 0) OR (RGU_BB = 0 AND RGU_TV = 0 AND RGU_VO = 1)  THEN '1P'
--    WHEN (RGU_BB = 1 AND RGU_TV = 1 AND RGU_VO = 0) OR (RGU_BB = 1 AND RGU_TV = 0 AND RGU_VO = 1) OR (RGU_BB = 0 AND RGU_TV = 1 AND RGU_VO = 1) THEN '2P'
--    WHEN (RGU_BB = 1 AND RGU_TV = 1 AND RGU_VO = 1) THEN '3P' END AS B_MixCode_Adj
    
    
    FROM UsefulFields c 
    WHERE date(dt) = DATE_TRUNC('Month', date(dt))
    
    --LEFT JOIN "db-analytics-dev"."dna_fixed_cr"  on c.act_acct_cd on --Tipo tecnologia es bb,tv,vo y/o _media o _tech  ON PD_BB_PROD_nm=ActivoInternet
    --WHERE date(dt) = DATE_TRUNC('Month', date(dt))
)


/*
,FinalCustomerBase_BOM AS( 
    SELECT *,
    CASE 
    WHEN B_Tech_Type like '%FTTH%' then 'FTTH'
    ELSE 'HFC' END AS B_TechAdj,
    CASE
    WHEN B_Tenure <=6 THEN 'Early Tenure'
    WHEN (B_Tenure >6 and b_tenure <= 12)  THEN 'Mid Tenure'
    when b_tenure > 12 then 'Late Tenure'
    ELSE NULL END AS B_FixedTenureSegment
    FROM CustomerBase_BOM 
)*/
--select distinct month, fixed_rejoiner, count(distinct AccountBOM) from FinalCustomerBase_BOM where b_date = '2022-09-02' group by 1,2 order by 1,2

,CustomerBase_EOM AS(
SELECT DISTINCT DATE_TRUNC('month', DATE_add('month', -1, cast(dt as date))) AS Month, dt as E_Date, act_acct_cd as AccountEOM, act_contact_phone_1 as E_Phone, pd_vo_prod_nm as E_VO_nm, 
    pd_tv_prod_nm as E_TV_nm, pd_bb_prod_nm as E_BB_nm, RGU_VO as E_RGU_VO, RGU_TV as E_RGU_TV, RGU_BB AS E_RGU_BB, fi_outst_age as E_Overdue, 
    TechFlag as E_TechFlag, C_CUST_AGE as E_Tenure, MinInst as E_MinInst,MaxInst as E_MaxInst, MIX AS E_MIX,
    (RGU_VO + RGU_TV + RGU_BB) AS E_NumRGUs, MORA AS E_MORA, mrcVO AS E_VO_MRC, mrcBB as E_BB_MRC, mrcTV as E_TV_MRC, avgmrc as E_AVG_MRC, BILL AS E_BILL_AMT,ACT_CUST_STRT_DT AS E_ACT_CUST_STRT_DT,
--    CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN '1P'
--    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) OR (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN '2P'
--    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN '3P' END AS E_Bundle_Type,
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
    --CASE WHEN (RGU_BB = 1 AND RGU_TV = 0 AND RGU_VO = 0) OR  (RGU_BB = 0 AND RGU_TV = 1 AND RGU_VO = 0) OR (RGU_BB = 0 AND RGU_TV = 0 AND RGU_VO = 1)  THEN '1P'
    --WHEN (RGU_BB = 1 AND RGU_TV = 1 AND RGU_VO = 0) OR (RGU_BB = 1 AND RGU_TV = 0 AND RGU_VO = 1) OR (RGU_BB = 0 AND RGU_TV = 1 AND RGU_VO = 1) THEN '2P'
    --WHEN (RGU_BB = 1 AND RGU_TV = 1 AND RGU_VO = 1) THEN '3P' END AS E_MixCode_Adj
    
    FROM UsefulFields c 
    WHERE date(dt) = DATE_TRUNC('month', date(dt))
    
    --LEFT JOIN `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-13_CR_CATALOGUE_TV_INTERNET_2021_T` ON PD_BB_PROD_nm=ActivoInternet
    --WHERE date(dt) = DATE_TRUNC('month', date(dt))
)

/*
,FinalCustomerBase_EOM AS(
    SELECT *,
    CASE 
    WHEN e_Tech_Type like '%FTTH%' then 'FTTH'
    ELSE 'HFC' END AS E_TechAdj, 
    CASE
    WHEN E_Tenure <=6 THEN 'Early Tenure'
     WHEN (E_Tenure >6 and e_tenure <= 12)  THEN 'Mid Tenure'
    when e_tenure > 12 then 'Late Tenure'
    ELSE NULL END AS E_FixedTenureSegment
    FROM CustomerBase_EOM 
)
*/

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
 AND date_diff('month',cast(Fixed_Month as timestamp),E_MaxInst)=0 
 THEN '04. Come Back to Life'
 WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC ('MONTH', E_ACT_CUST_STRT_DT) = Fixed_Month) THEN '05. New Customer'
 WHEN ActiveBOM = 1 AND ActiveEOM = 0 THEN '06. Loss'
 END AS MainMovement,
 E_RGU_BB - B_RGU_BB as DIF_RGU_BB , E_RGU_TV - B_RGU_TV as DIF_RGU_TV , E_RGU_VO - B_RGU_VO as DIF_RGU_VO , E_NumRGUs - B_NumRGUs as DIF_TOTAL_RGU
 FROM FixedCustomerBase f
)



,SPINMOVEMENTBASE AS (
    SELECT b.*,
    CASE 
    WHEN MainMovement='Same RGUs' AND (E_BILL_AMT - B_BILL_AMT) > 0 THEN '1. Up-spin' 
    WHEN MainMovement='Same RGUs' AND (E_BILL_AMT - B_BILL_AMT) < 0 THEN '2. Down-spin' 
    ELSE '3. No Spin' END AS SpinMovement
    FROM MAINMOVEMENTBASE b
)



--------------------------------------------------------------------------- Fixed Churn Flags -------------------------------------------------------------------------------------------
------------------------------------------Voluntary & Involuntary-------------------------------------------------------------
,MAX_SO_CHURN AS(
 SELECT DISTINCT account_name AS CONTRATOSO, DATE_TRUNC('Month',MAX(order_start_date)) as DeinstallationMonth, MAX(order_start_date) AS FECHA_CHURN
 FROM "db-stage-dev"."so_cr"
 WHERE
  order_type = 'DESINSTALACION' 
  AND (order_status <> 'CANCELADA' OR order_status <> 'ANULADA')
 AND order_start_date IS NOT NULL
 GROUP BY 1
)

,CHURNERSSO AS(
  SELECT DISTINCT account_name AS CONTRATOSO, DATE_TRUNC('Month',order_start_date) as DeinstallationMonth,
  order_start_date as DeinstallationDate,
  CASE WHEN command_id like '%MOROSIDAD%' THEN 'Involuntary'
  WHEN command_id not like  '%MOROSIDAD%' THEN 'Voluntary'
  END AS Submotivo
 FROM "db-stage-dev"."so_cr" t
 INNER JOIN MAX_SO_CHURN m on account_name = m.contratoso and order_start_date = fecha_churn
 WHERE
  order_type = 'DESINSTALACION'
  AND (order_status <> 'CANCELADA' OR order_status <> 'ANULADA')
 AND order_start_date IS NOT NULL
)

,MaximaFecha as(
  select distinct  act_acct_cd, max(dt) as MaxFecha FROM "db-analytics-dev"."dna_fixed_cr"
  group by 1
)

,ChurnersJoin as(
select Distinct f.dt,f.act_acct_cd,Submotivo,DeinstallationMonth,DeinstallationDate,MaxFecha 
FROM "db-analytics-dev"."dna_fixed_cr" f
left join churnersso c on contratoso=f.act_acct_cd and date_trunc('Month',cast(dt as date))=DeinstallationMonth
left join MaximaFecha m on f.act_acct_cd=m.act_acct_cd
)

,MaxFechaJoin as(
select dt,DeinstallationMonth as DxMonth,act_acct_cd,
CASE WHEN date_diff('month',cast(MaxFecha as timestamp),DeinstallationMonth)<=1 THEN Submotivo
ELSE NULL END AS FixedChurnTypeFlag
FROM Churnersjoin
WHERE Submotivo IS NOT NULL
)


,ChurnersFixedTable as(
select f.*,FixedChurnTypeFlag FROM SPINMOVEMENTBASE f left join MaxFechaJoin b
on Fixed_Month=date_trunc('Month',b.DxMonth) and Fixed_Account=b.act_acct_cd
)


--------------------------------------------------------------------------- Rejoiners -------------------------------------------------------------
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
,CASE WHEN RejoinerMonth>=date('2022-05-01') AND RejoinerMonth<=DATE_ADD('MONTH', 1, date('2022-05-01') ) THEN 1 ELSE 0 END AS Fixed_PR
FROM FixedCustomerBase f LEFT JOIN InactiveUsersMonth i ON f.Fixed_Account=i.Fixed_Account AND Fixed_Month=ExitMonth
)
,FixedRejoinerFebPopulation AS(
SELECT DISTINCT Fixed_Month,RejoinerPopFlag,Fixed_PR,Fixed_Account,date('2022-05-01') AS Month
FROM RejoinersPopulation
WHERE RejoinerPopFlag=1
AND Fixed_PR=1
AND Fixed_Month<>date('2022-05-01')
GROUP BY 1,2,3,4
)
,FullFixedBase_Rejoiners AS(
SELECT DISTINCT f.*,Fixed_PR
,CASE WHEN Fixed_PR=1 AND MainMovement='Come Back to Life'
THEN 1 ELSE 0 END AS Fixed_Rejoiner
FROM ChurnersFixedTable f LEFT JOIN FixedRejoinerFebPopulation r ON f.Fixed_Account=r.Fixed_Account AND f.Fixed_Month=CAST(r.Month AS DATE)
)
,FinalTable as(
SELECT *,CASE
WHEN FixedChurnTypeFlag is not null THEN b_NumRGUs
WHEN MainMovement='Downsell' THEN (B_NumRGUs - E_NumRGUs)
ELSE NULL END AS RGU_Churn,
CONCAT(B_VO_nm,B_TV_nm,B_BB_nm) AS B_PLAN,CONCAT(E_VO_nm,E_TV_nm,E_BB_nm) AS E_PLAN
FROM FullFixedBase_Rejoiners
)

Select Fixed_Month,MainMovement,count(distinct Fixed_Account) From FinalTable
where activeeom=1
group by 1,2
order by 1,2
