----------------------------------------Customer Base----------------------------------------------------------
WITH
UsefulFields AS(
SELECT ACT_ACCT_CD, DT,act_cust_strt_dt, PD_VO_PROD_CD, PD_BB_PROD_CD, PD_TV_PROD_CD, fi_outst_age, MAX(SAFE_CAST(SAFE_CAST(act_cust_strt_dt AS TIMESTAMP) AS DATE)) AS MaxStart,
avg(FI_VO_MRC_AMT) AS avgVO, avg(FI_BB_MRC_AMT) as avgBB, avg(FI_TV_MRC_AMT) AS avgTV,
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
  WHERE PD_MIX_CD<>"0P"
  GROUP BY 1,2,3,4,5,6, FI_OUTST_AGE
  ORDER BY dt
)
,ActiveUsersBOM AS(
SELECT DISTINCT DATE_TRUNC(DATE(DT),Month) AS Month, ACT_ACCT_CD AS accountBOM,dt
FROM UsefulFields
WHERE dt="2022-02-02" AND (safe_cast(fi_outst_age as int64) <= 90 OR fi_outst_age IS NULL) 
GROUP BY 1,2,3
)
,ActiveUsersEOM AS(
SELECT DISTINCT DATE_TRUNC(DATE_SUB(DATE(DT),INTERVAL 1 MONTH),MONTH) AS Month, ACT_ACCT_CD AS accountEOM,dt, PD_VO_PROD_CD, PD_BB_PROD_CD, PD_TV_PROD_CD,
avgVO, avgBB, avgTV,
FROM UsefulFields
WHERE dt='2022-03-02' AND (safe_cast(fi_outst_age as int64) <= 90 OR fi_outst_age IS NULL) 
GROUP BY 1,2,3,PD_VO_PROD_CD, PD_BB_PROD_CD, PD_TV_PROD_CD, avgVO, avgBB, avgTV
)
,CustomerStatus AS(
  SELECT DISTINCT 
   CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
  END AS Month,
      CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
  END AS account,
  CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM,
  CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM,
  PD_VO_PROD_CD, PD_BB_PROD_CD, PD_TV_PROD_CD,
  FROM ActiveUsersBOM b FULL OUTER JOIN ActiveUsersEOM e
  ON b.accountBOM = e.accountEOM AND b.MONTH = e.MONTH
  )
--------------------------------------Involuntary Churners---------------------------------------------------
/*,CUSTOMERS_FIRSTLAST_RECORD AS(
 SELECT DISTINCT DATE_TRUNC(dt,month) AS MES, act_acct_cd AS Account, Min(dt) as FirstCustRecord, Max(dt) as LastCustRecord
 FROM UsefulFields
 WHERE dt="2022-03-01"
 GROUP BY MES, account
 ORDER BY MES
 )*/
,FIRSTCUSTRECORD AS (
    SELECT DATE_TRUNC(dt,month) AS MES, act_acct_cd AS Account, dt AS FirstCustRecord
    FROM UsefulFields 
    WHERE dt="2022-02-02"
)
,LastCustRecord as(
    SELECT  DATE_TRUNC(dt,month) AS MES, act_acct_cd AS Account, dt as LastCustRecord
    FROM UsefulFields 
    WHERE dt="2022-03-02"
)


 ,NO_OVERDUE AS(
 SELECT DISTINCT DATE_TRUNC (dt, MONTH) AS MES, act_acct_cd AS Account, fi_outst_age
 FROM UsefulFields t
 INNER JOIN FIRSTCUSTRECORD  r ON t.dt = r.FirstCustRecord and r.account = t.act_acct_cd
 WHERE safe_cast(fi_outst_age as int64) <= 90
 GROUP BY MES, account, fi_outst_age
)
 ,OVERDUELASTDAY AS(
 SELECT DISTINCT DATE_TRUNC(DATE_SUB(DATE(DT),INTERVAL 1 MONTH),MONTH) AS MES, act_acct_cd AS Account, fi_outst_age,
 (date_diff(dt, MaxStart, DAY)) as ChurnTenureDays
 FROM UsefulFields t
 INNER JOIN LastCustRecord r ON t.dt = r.LastCustRecord and r.account = t.act_acct_cd
 WHERE  safe_cast(fi_outst_age as int64) >= 90
 GROUP BY MES, account, fi_outst_age, ChurnTenureDays
 )

 ,INVOLUNTARYNETCHURNERS AS(
 SELECT DISTINCT n.MES AS Month, n. account, l.ChurnTenureDays
 FROM NO_OVERDUE n INNER JOIN OVERDUELASTDAY l ON n.account = l.account and n.MES = l.MES
)
,InvoluntaryChurners AS(
SELECT DISTINCT Month, SAFE_CAST(Account AS STRING) AS Account, ChurnTenureDays
,CASE WHEN Account IS NOT NULL THEN "2. Fixed Involuntary Churner" END AS ChurnerType
FROM INVOLUNTARYNETCHURNERS 
GROUP BY Month, Account,ChurnerType, ChurnTenureDays
)
SELECT Month, COUNT(ChurnerType) AS InvoluntaryChurners
FROM InvoluntaryChurners
GROUP BY Month
ORDER BY Month
