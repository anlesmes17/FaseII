WITH 

Fields AS(
SELECT account_id,dt
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_postpaid_history_v2` 
WHERE org_id = "338" AND account_type ="Residential"
AND account_status NOT IN('Ceased','Closed','Recommended for cease')
)
,ActiveUsersBOM AS(
SELECT DISTINCT DATE_TRUNC(DATE_ADD(dt, INTERVAL 1 MONTH),MONTH) AS Month, account_id AS accountBOM,dt
FROM Fields
WHERE DATE(dt) = LAST_DAY(dt, MONTH)
GROUP BY 1,2,3
)
,ActiveUsersEOM AS(
SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, account_id AS accountEOM,dt
FROM Fields
WHERE DATE(dt) = LAST_DAY(DATE(dt), MONTH)
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
SELECT DISTINCT a.account RejoinerAccount, b.account
FROM PotentialRejoiner a  INNER JOIN RejoinerFebSummary b on a.account=b.account AND a.Month=b.Month
)
SELECT count(RejoinerAccount) AS RejoinerPopulation,  
FROM PreliminaryTable 
