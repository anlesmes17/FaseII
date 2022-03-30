WITH
UsefulFields AS(
SELECT ACT_ACCT_CD, DT,act_cust_strt_dt, PD_VO_PROD_CD, PD_BB_PROD_CD, PD_TV_PROD_CD,
avg(FI_VO_MRC_AMT) AS avgVO, avg(FI_BB_MRC_AMT) as avgBB, avg(FI_TV_MRC_AMT) AS avgTV,
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_fixed_history_v2` 
  WHERE ACT_CUST_TYP_NM="Residencial" AND PD_MIX_CD<>"0P"
  GROUP BY 1,2,3,4,5,6
)
,ActiveUsersBOM AS(
SELECT DISTINCT DATE_TRUNC(DATE(DT),Month) AS Month, ACT_ACCT_CD AS accountBOM,dt
FROM UsefulFields
WHERE dt=DATE_TRUNC(DT, Month)
GROUP BY 1,2,3
)
,ActiveUsersEOM AS(
SELECT DISTINCT DATE_TRUNC(DATE(DT),MONTH) AS Month, ACT_ACCT_CD AS accountEOM,dt, PD_VO_PROD_CD, PD_BB_PROD_CD, PD_TV_PROD_CD,
avgVO, avgBB, avgTV,
FROM UsefulFields
WHERE dt=LAST_DAY(dt,Month)
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
),

-----------------------------------------RGU Closing Base-----------------------------------------------------
RGUCOUNT AS(
SELECT *,
CASE WHEN PD_VO_PROD_CD IS NOT NULL AND PD_VO_PROD_CD <>"" THEN "VO" ELSE NULL END AS VO,
CASE WHEN PD_BB_PROD_CD IS NOT NULL AND PD_BB_PROD_CD <>"" THEN "BB"ELSE NULL END AS BB,
CASE WHEN PD_TV_PROD_CD IS NOT NULL AND PD_TV_PROD_CD <>"" THEN "TV" ELSE NULL END AS TV,
FROM CustomerStatus
)
--SELECT COUNT(VO) AS VO_RGU, COUNT(BB) AS BB_RGU, COUNT(TV) AS TV_RGU
--FROM RGUCOUNT 
-------------------------------------------Gross Adds---------------------------------------------------------

,GrossAddsMonth AS (
SELECT DISTINCT act_acct_cd, min(safe_cast(safe_cast(act_cust_strt_dt as timestamp) as date)) AS MinStartDate,DATE_TRUNC(dt,MONTH) AS Month,
PD_VO_PROD_CD, PD_BB_PROD_CD, PD_TV_PROD_CD, avgVO, avgBB, avgTV
FROM UsefulFields 
WHERE dt=LAST_DAY(dt,month)
GROUP BY act_acct_cd,Month, PD_VO_PROD_CD, PD_BB_PROD_CD, PD_TV_PROD_CD, avgVO, avgBB, avgTV
HAVING MinStartDate>=Month
),
GrossAddsRGUs AS (
SELECT act_acct_cd,avgVO, avgBB, avgTV,
CASE WHEN PD_VO_PROD_CD IS NOT NULL AND PD_VO_PROD_CD <>"" THEN 1 ELSE NULL END AS VO,
CASE WHEN PD_BB_PROD_CD IS NOT NULL AND PD_BB_PROD_CD <>"" THEN 1 ELSE NULL END AS BB,
CASE WHEN PD_TV_PROD_CD IS NOT NULL AND PD_VO_PROD_CD <>"" THEN 1 ELSE NULL END AS TV,
FROM GROSSADDSMONTH
GROUP BY act_acct_cd, vo,bb,tv,avgVO, avgBB, avgTV
)
SELECT count(act_acct_cd) AS GrossAdds, sum(VO) AS GrosAddsVO, sum(BB) AS GrossAddsBB, sum(TV) AS GrossAddsTV,
round(sum(avgVO),0) AS RevenueVO, round(sum(avgBB),0) AS RevenueBB, round(sum(avgTV),0) AS RevuenueTV
FROM GrossAddsRGUs
----------------------------------------------------------------------------------------------------------------
