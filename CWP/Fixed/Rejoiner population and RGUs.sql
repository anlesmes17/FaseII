WITH 

Fields AS(
SELECT act_acct_cd,dt, safe_cast(fi_outst_age as int64) as Overdue, PD_VO_PROD_CD, PD_BB_PROD_CD, PD_TV_PROD_CD
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2`  
WHERE PD_MIX_CD<>"0P" 
  AND fi_tot_mrc_amt IS NOT NULL AND SAFE_CAST(fi_tot_mrc_amt AS FLOAT64) <> 0
)
,ActiveUsersBOM AS(
SELECT DISTINCT DATE_TRUNC(DATE_ADD(dt, INTERVAL 1 MONTH),MONTH) AS Month, act_acct_cd AS accountBOM,dt, PD_VO_PROD_CD, PD_BB_PROD_CD, PD_TV_PROD_CD
FROM Fields
WHERE (Overdue<=90 OR Overdue IS NULL) AND DATE(dt) = LAST_DAY(dt, MONTH)
GROUP BY 1,2,3, PD_VO_PROD_CD, PD_BB_PROD_CD, PD_TV_PROD_CD
)
,ActiveUsersEOM AS(
SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, act_acct_cd AS accountEOM,dt
FROM Fields
WHERE (Overdue<=90 OR Overdue IS NULL) AND DATE(dt) = LAST_DAY(dt, MONTH)
GROUP BY 1,2,3
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
  CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM, PD_VO_PROD_CD, PD_BB_PROD_CD, PD_TV_PROD_CD
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
)
,JoinRejoiners AS(
SELECT DISTINCT a.account RejoinerAccount, b.account, a.month, PD_VO_PROD_CD, PD_BB_PROD_CD, PD_TV_PROD_CD
FROM PotentialRejoiner a  INNER JOIN RejoinerFebSummary b on a.account=b.account AND a.Month=b.Month
WHERE a.Month="2022-02-01" 
) 
,RGUCOUNT AS(
SELECT *,
CASE WHEN pd_vo_prod_cd is not null and pd_vo_prod_cd<>"nan" Then "VO" ELSE NULL END AS VO,
CASE WHEN pd_bb_prod_cd is not null and pd_bb_prod_cd<>"nan" Then "BB" ELSE NULL END AS BB,
CASE WHEN pd_tv_prod_cd is not null and pd_tv_prod_cd<>"nan" Then "TV" ELSE NULL END AS TV,
FROM JoinRejoiners

)
SELECT month, count(account) AS RejoinerPopulation, count(VO) AS VO_RGU, Count(BB) AS BB_RGU, Count (TV) AS TV_RGU  
FROM RGUCOUNT
GROUP BY Month
order by month
