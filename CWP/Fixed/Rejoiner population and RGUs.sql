WITH 

Fields AS(
 SELECT DISTINCT act_acct_cd,dt,safe_cast(fi_outst_age as int64) AS fi_outst_age
 , pd_vo_prod_cd, pd_bb_prod_cd, pd_tv_prod_cd,
 FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
 WHERE PD_MIX_CD<>"0P" 
  AND fi_tot_mrc_amt IS NOT NULL AND SAFE_CAST(fi_tot_mrc_amt AS FLOAT64) <> 0
 GROUP BY act_acct_cd,dt,fi_outst_age, pd_vo_prod_cd, pd_bb_prod_cd, pd_tv_prod_cd
)
,ActiveUsersBOM AS(
SELECT DISTINCT DATE_TRUNC(DATE_ADD(dt, INTERVAL 1 MONTH),MONTH) AS Month, act_acct_cd AS accountBOM,dt
pd_vo_prod_cd, pd_bb_prod_cd, pd_tv_prod_cd,
FROM Fields
WHERE (fi_outst_age<=90 OR fi_outst_age IS NULL) AND DATE(dt) = LAST_DAY(dt, MONTH)
GROUP BY 1,2,3, pd_vo_prod_cd, pd_bb_prod_cd, pd_tv_prod_cd
)
,ActiveUsersEOM AS(
SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, act_acct_cd AS accountEOM,dt
FROM Fields
WHERE fi_outst_age<=90 OR fi_outst_age IS NULL AND DATE(dt) = LAST_DAY(DATE(dt), MONTH)
GROUP BY 1,2,3
)
,CustomerStatus AS(
  SELECT DISTINCT b.pd_vo_prod_cd, b.pd_bb_prod_cd, b.pd_tv_prod_cd,
   CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
  END AS Month,
      CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
  END AS account,
  CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM,
  CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM,
  FROM ActiveUsersBOM b FULL OUTER JOIN ActiveUsersEOM e
  ON b.accountBOM = e.accountEOM AND b.MONTH = e.MONTH
)
,PotentialRejoiner AS(
SELECT *
,CASE WHEN ActiveBOM=1 AND ActiveEOM=0 THEN DATE_ADD(Month, INTERVAL 4 MONTH) END AS PR
FROM CustomerStatus
ORDER BY account,Month
)
,PotentialRejoinersFeb AS(
SELECT *
,CASE WHEN PR>='2022-02-01' AND PR<=DATE_ADD('2022-02-01',INTERVAL 4 MONTH) THEN 1 ELSE 0 END AS PRFeb
FROM PotentialRejoiner
)
,RejoinerFebSummary AS (
SELECT DISTINCT account,Month
FROM PotentialRejoinersFeb 
WHERE PRfeb=1
ORDER BY account
),
PreliminaryTable AS(
SELECT DISTINCT a.account RejoinerAccount, b.account, pd_vo_prod_cd, pd_bb_prod_cd, pd_tv_prod_cd, 
FROM PotentialRejoiner a  INNER JOIN RejoinerFebSummary b on a.account=b.account AND a.Month=b.Month
)
SELECT count(RejoinerAccount) AS RejoinerPopulation, count(pd_vo_prod_cd) AS RGU_VO, count(pd_bb_prod_cd) AS RGU_BB, count(pd_tv_prod_cd) AS RGU_TV, 
FROM PreliminaryTable 
