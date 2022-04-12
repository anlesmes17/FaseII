--CREATE OR REPLACE TABLE 
--`gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-03-30_CWC_Fixed_DashboardInput` AS
WITH
  UsefulFields AS(
  SELECT
    DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, Max(dt) as MaxDateMonth,
    dt,act_acct_cd, act_contact_phone_1, act_contact_phone_2, act_contact_phone_3,
    pd_mix_cd,pd_mix_nm,pd_bb_prod_nm,pd_tv_prod_nm,pd_vo_prod_nm,
   CASE WHEN IS_NAN (safe_cast(fi_tot_mrc_amt AS float64)) THEN 0
    WHEN NOT IS_NAN (safe_cast(fi_tot_mrc_amt AS float64)) THEN ROUND((safe_cast(fi_tot_mrc_amt AS float64)),0)
    END AS mrc_amt,
    fi_outst_age, fi_tot_srv_chrg_amt, ROUND(safe_cast(fi_bb_mrc_amt as float64),0) as fi_bb_mrc_amt, ROUND(safe_cast(fi_tv_mrc_amt as float64),0) as fi_tv_mrc_amt, ROUND(safe_cast(fi_vo_mrc_amt as float64),0) as fi_vo_mrc_amt,
    MAX(act_cust_strt_dt) AS MaxStart, bundle_code, bundle_name,
    CASE WHEN pd_bb_prod_nm IS NOT NULL AND pd_bb_prod_nm <> "" AND pd_bb_prod_nm NOT LIKE '%Benefit%' THEN 1 ELSE 0 END AS numBB,
    CASE WHEN pd_tv_prod_nm IS NOT NULL AND pd_tv_prod_nm <> "" AND pd_tv_prod_nm NOT LIKE '%Benefit%' THEN 1 ELSE 0 END AS numTV,
    CASE WHEN pd_vo_prod_nm IS NOT NULL AND pd_vo_prod_nm <> "" AND pd_vo_prod_nm NOT LIKE '%Benefit%' THEN 1 ELSE 0 END AS numVO,
    CASE WHEN LENGTH(CAST(act_acct_cd AS STRING))=8 THEN "HFC"
    WHEN (pd_tv_accs_media = "COPPER" OR pd_vo_accs_media = "COPPER") THEN "COPPER"
    WHEN (pd_tv_accs_media = "FIBER" OR pd_vo_accs_media = "FIBER") THEN "FTTH"
    WHEN pd_bb_prod_nm LIKE "%GPON%"OR pd_bb_prod_nm LIKE "%FTT%" THEN "FTTH"
    WHEN (pd_bb_prod_nm IS NOT NULL AND pd_bb_prod_nm <>"") AND (pd_bb_prod_nm NOT IN ("%GPON%", '%FTT%')) THEN "COPPER"
  ELSE "Not Classified" END AS Techonology_type
  FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwc_info_dna_fixed_historic_v2` 
  WHERE
    org_cntry="Jamaica" AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence','Standard')
    AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W')
   -- AND act_segment_prpty_nm NOT IN ('Business')--, 'Managed Business', 'Enterprise')--, 'Standard Commerical' )
  GROUP BY
    Month,dt,act_acct_cd,
    pd_mix_cd,pd_mix_nm,pd_bb_prod_nm,pd_tv_prod_nm,pd_vo_prod_nm,
    mrc_amt ,fi_outst_age, fi_tot_srv_chrg_amt, fi_bb_mrc_amt, fi_tv_mrc_amt, fi_vo_mrc_amt, 
    Techonology_type, numBB, numTV, numVO, bundle_code, bundle_name, act_contact_phone_1, act_contact_phone_2, act_contact_phone_3
),
  AverageMRC_User AS(
  SELECT DISTINCT DATE_TRUNC(DATE(dt),MONTH) AS Month, act_acct_cd, round(avg(mrc_amt),0)  AS AvgMRC
  FROM UsefulFields
  WHERE mrc_amt IS NOT NULL AND mrc_amt <> 0
  GROUP BY Month, act_acct_cd
  ),
  ActiveUsersBOM AS(
  SELECT
    DISTINCT DATE_TRUNC(DATE_ADD(u.dt, INTERVAL 1 MONTH),MONTH) AS Month, u.act_acct_cd AS accountBOM, act_contact_phone_1 as PhoneBOM1, act_contact_phone_2 as PhoneBOM2, act_contact_phone_3 as PhoneBOM3,
     u.dt as B_Date,pd_mix_cd as B_MixCode ,pd_mix_nm as B_MixName ,pd_bb_prod_nm as B_ProdBBName,pd_tv_prod_nm as B_ProdTVName,pd_vo_prod_nm as B_ProdVoName,
    (NumBB+NumTV+NumVO) as B_NumRGUs, 
    CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN "BO"
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN "TV"
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN "VO"
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN "BO+TV"
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN "BO+VO"
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN "VO+TV"
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN "BO+VO+TV"
    END AS B_MixName_Adj,
    CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN "1P"
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN "2P"
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN "3P" END AS B_MixCode_Adj,
    mrc_amt as B_MRC ,fi_outst_age  as B_OutstAge, fi_tot_srv_chrg_amt as B_MRCAdj, fi_bb_mrc_amt as B_MRCBB, fi_tv_mrc_amt as B_MRCTV, fi_vo_mrc_amt as B_MRCVO,
    MaxStart as B_MaxStart, Techonology_type as B_Tech_Type, bundle_code as B_bundlecode, bundle_name as B_bundlename, AvgMRC as B_Avg_MRC
 FROM
    UsefulFields u LEFT JOIN AverageMRC_User a ON u.act_acct_cd = a.act_acct_cd AND u.Month = a.Month
  WHERE
    (SAFE_CAST(fi_outst_age AS numeric) <= 90
      OR fi_outst_age IS NULL)
    AND 
    DATE(u.dt) = LAST_DAY(u.dt, MONTH)
    AND ((AvgMRC IS NOT NULL AND AvgMRC <> 0 AND DATE_DIFF(MaxDateMonth, MaxStart, DAY)>60) OR  (DATE_DIFF(MaxDateMonth, MaxStart, DAY)<=60))
  GROUP BY
    Month, accountBOM, phoneBOM1, phoneBOM2, phoneBOM3,B_Date,B_MixCode ,B_MixName, B_ProdBBName,B_ProdTVName,B_ProdVoName, 
    B_MRC ,B_OutstAge, B_MRCAdj, B_MRCBB, B_MRCTV, B_MRCVO, B_MaxStart, B_Tech_Type, B_NumRGUs, B_bundlecode, B_bundlename, B_Avg_MRC, B_MixName_Adj, B_MixCode_Adj
),
  ActiveUsersEOM AS ( 
  SELECT
    DISTINCT DATE_TRUNC(DATE(u.dt),MONTH) AS Month, u.act_acct_cd AS accountEOM, act_contact_phone_1 as PhoneEOM1, act_contact_phone_2 as PhoneEOM2, act_contact_phone_3 as PhoneEOM3,
    u.dt as E_Date,pd_mix_cd as E_MixCode ,pd_mix_nm as E_MixName ,pd_bb_prod_nm as E_ProdBBName,pd_tv_prod_nm as E_ProdTVName,pd_vo_prod_nm as E_ProdVoName,
     (NumBB+NumTV+NumVO) as E_NumRGUs,
     CASE WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 0 THEN "BO"
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 0 THEN "TV"
    WHEN NumBB = 0 AND NumTV = 0 AND NumVO = 1 THEN "VO"
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 0 THEN "BO+TV"
    WHEN NumBB = 1 AND NumTV = 0 AND NumVO = 1 THEN "BO+VO"
    WHEN NumBB = 0 AND NumTV = 1 AND NumVO = 1 THEN "VO+TV"
    WHEN NumBB = 1 AND NumTV = 1 AND NumVO = 1 THEN "BO+VO+TV"
    END AS E_MixName_Adj,
    CASE WHEN (NumBB = 1 AND NumTV = 0 AND NumVO = 0) OR  (NumBB = 0 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 0 AND NumTV = 0 AND NumVO = 1)  THEN "1P"
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 0) OR (NumBB = 1 AND NumTV = 0 AND NumVO = 1) OR (NumBB = 0 AND NumTV = 1 AND NumVO = 1) THEN "2P"
    WHEN (NumBB = 1 AND NumTV = 1 AND NumVO = 1) THEN "3P" END AS E_MixCode_Adj,
     mrc_amt as E_MRC ,fi_outst_age  as E_OutstAge, fi_tot_srv_chrg_amt as E_MRCAdj, fi_bb_mrc_amt as E_MRCBB, fi_tv_mrc_amt as E_MRCTV, fi_vo_mrc_amt as E_MRCVO,
    MaxStart as E_MaxStart, Techonology_type as E_Tech_TypE, bundle_code as E_bundlecode, bundle_name as E_bundlename, AvgMRC as E_Avg_MRC
 FROM
    UsefulFields u LEFT JOIN AverageMRC_User a ON u.act_acct_cd = a.act_acct_cd AND u.Month = a.Month
  WHERE
    (SAFE_CAST(fi_outst_age AS numeric) <= 90
      OR fi_outst_age IS NULL)
    AND 
    DATE(u.dt) = LAST_DAY(u.dt, MONTH)
    AND ((AvgMRC IS NOT NULL AND AvgMRC <> 0 AND DATE_DIFF(MaxDateMonth, MaxStart, DAY)>60) OR  (DATE_DIFF(MaxDateMonth, MaxStart, DAY)<=60))
  GROUP BY
    Month, accountEOM, PhoneEOM1, PhoneEOM2, PhoneEOM3, E_Date,E_MixCode ,E_MixName, E_ProdBBName,E_ProdTVName,E_ProdVoName, 
    E_MRC ,E_OutstAge, E_MRCAdj, E_MRCBB, E_MRCTV, E_MRCVO, E_MaxStart, E_Tech_Type, E_NumRGUs, E_bundlecode, E_bundlename, E_Avg_MRC, E_MixName_Adj, E_MixCode_Adj
),
  CUSTOMERBASE AS(
  SELECT DISTINCT
   CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
  END AS Fixed_Month,
      CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
  END AS Fixed_Account,
   CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN phoneBOM1
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN phoneEOM1
  END AS f_contactphone1,
  CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN phoneBOM2
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN phoneEOM2
  END AS f_contactphone2,
  CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN phoneBOM3
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN phoneEOM3
  END AS f_contactphone3,
  CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM,
  CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM,
  B_Date,B_Tech_Type, B_MixCode, B_MixCode_Adj, B_MixName, B_MixName_Adj,  B_ProdBBName,B_ProdTVName,B_ProdVoName,B_NumRGUs,B_bundlecode, B_bundlename,
  B_MRC ,B_OutstAge, B_MRCAdj, B_MRCBB, B_MRCTV, B_MRCVO, B_Avg_MRC, B_MaxStart, DATE_DIFF(B_Date, B_MaxStart, DAY) as B_TenureDays,
  CASE WHEN DATE_DIFF(B_Date, B_MaxStart, DAY) <= 180 Then "Early-Tenure"
  WHEN DATE_DIFF(B_Date, B_MaxStart, DAY) > 180 THEN "Late-Tenure" END AS B_FixedTenureSegment,
  E_Date,E_Tech_Type, E_MixCode, E_MixCode_Adj ,E_MixName, E_MixName_Adj ,E_ProdBBName,E_ProdTVName,E_ProdVoName, E_NumRGUs, E_bundlecode, E_bundlename,
  E_MRC ,E_OutstAge, E_MRCAdj, E_MRCBB, E_MRCTV, E_MRCVO, E_Avg_MRC, E_MaxStart, DATE_DIFF(E_Date, E_MaxStart, DAY) as E_TenureDays,
  CASE WHEN DATE_DIFF(E_Date, E_MaxStart, DAY) <= 180 Then "Early-Tenure"
  WHEN DATE_DIFF(E_Date, E_MaxStart, DAY) > 180 THEN "Late-Tenure" END AS E_FixedTenureSegment,
  (E_MRC - B_MRC) as MRCDiff
  FROM ActiveUsersBOM b FULL OUTER JOIN ActiveUsersEOM e
  ON b.accountBOM = e.accountEOM AND b.MONTH = e.MONTH
  ORDER BY Fixed_Account
),
############################################# CUSTOMERMOVEMENTS ######################################################################################################################

MAINMOVEMENTBASE AS(
SELECT a.*,
CASE
WHEN (E_NumRGUs - B_NumRGUs) = 0 THEN "1.SameRGUs" 
WHEN (E_NumRGUs - B_NumRGUs) > 0 THEN "2.Upsell"
WHEN (E_NumRGUs - B_NumRGUs) < 0 THEN "3.Downsell"
WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC (E_MaxStart, MONTH) = '2022-02-01') THEN "4.New Customer"
WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC (E_MaxStart, MONTH) <> '2022-02-01') THEN "5.Come Back to Life"
WHEN (B_NumRGUs > 0 AND E_NumRGUs IS NULL) THEN "6.Null last day"
WHEN B_NumRGUs IS NULL AND E_NumRGUs IS NULL THEN "7.Always null"
END AS MainMovement
FROM CUSTOMERBASE a
),

SPINMOVEMENTBASE AS(
  SELECT b.*,
  CASE WHEN MainMovement = "1.SameRGUs" AND (E_MRC - B_MRC) > 0 THEN "1. Up-spin"
  WHEN MainMovement = "1.SameRGUs" AND (E_MRC - B_MRC) < 0 THEN "2. Down-spin"
  ELSE "3. No Spin" END AS SpinMovement
  FROM MAINMOVEMENTBASE b
),

################################# FIXED CHURN FLAGS ###############################################################

---------------------------------------------------Voluntary Churners-------------------------------------------------------------------
-- Last churn date on the voluntary churn base per customer
MAXFECHACHURNMES AS(
SELECT DISTINCT src_account_id, PARSE_DATE("%Y%m%d",max(reporting_date_key)) AS MaxFecha
-- ESTA BASE TIENE SÓLO FEBRERO - TOCA PEDIR ENERO
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-03-17_Jamaica_VoluntaryChurners_v2` 
GROUP BY src_account_id
),
-- Number of churned RGUs on the maximum date - it doesn't consider mobile
FIXEDCHURNEDRGUS AS(
SELECT DISTINCT DATE_TRUNC(MaxFecha, MONTH) AS ChurnMonth, t.src_account_id, count(*) as NumChurns
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-03-17_Jamaica_VoluntaryChurners_v2` t
INNER JOIN MAXFECHACHURNMES m ON t.src_account_id = m.src_account_id AND PARSE_DATE("%Y%m%d",t.reporting_date_key) = MaxFecha
WHERE lob <> "Mobile Postpaid"
GROUP BY src_account_id, ChurnMonth
ORDER BY NumChurns desc
),
-- Number of RGUs a customer has on the last record of the month
RGUSLastRecordDNA AS(
SELECT DISTINCT DATE_TRUNC(dt, MONTH) AS Month, act_acct_cd,
CASE WHEN last_value(pd_mix_nm) over(partition by act_acct_cd, DATE_TRUNC(dt, MONTH) order by dt) IN ('VO', 'BO', 'TV') THEN 1
WHEN last_value(pd_mix_nm) over(partition by act_acct_cd, DATE_TRUNC(dt, MONTH) order by dt) IN ('BO+VO', 'BO+TV', 'VO+TV') THEN 2
WHEN last_value(pd_mix_nm) over(partition by act_acct_cd, DATE_TRUNC(dt, MONTH) order by dt) IN ('BO+VO+TV') THEN 3
ELSE 0 END AS NumRgusLastRecord,
FROM Usefulfields
WHERE (fi_outst_age <= 90 OR fi_outst_age IS NULL) 
 ORDER BY act_acct_cd
),
-- Date of the last record of the month per customer
LastRecordDateDNA AS(
SELECT DISTINCT DATE_TRUNC(dt, MONTH) AS Month, act_acct_cd,max(dt) as LastDate
FROM Usefulfields
WHERE  (fi_outst_age <= 90 OR fi_outst_age IS NULL) 
 GROUP BY MONTH, act_acct_cd
 ORDER BY act_acct_cd 
),
-- Number of outstanding days on the last record date
OverdueLastRecordDNA AS(
SELECT DISTINCT DATE_TRUNC(dt, MONTH) AS Month, t.act_acct_cd, fi_outst_age as LastOverdueRecord,
(date_diff(dt, MaxStart, DAY)) as ChurnTenureDays
FROM Usefulfields t 
INNER JOIN LastRecordDateDNA d ON t.act_acct_cd = d.act_acct_cd AND t.dt = d.LastDate
),
-- Total Voluntary Churners considering number of churned RGUs, outstanding age and churn date
VoluntaryTotalChurners AS(
SELECT distinct l.Month, l.act_acct_cd, d.LastDate, o.ChurnTenureDays,
CASE WHEN length(cast(l.act_acct_cd AS STRING)) = 12 THEN "1. Liberate"
ELSE "2. Cerilion" END AS BillingSystem,
CASE WHEN (d.LastDate = date_trunc(d.LastDate, Month) or d.LastDate = LAST_DAY(d.LastDate, MONTH)) THEN "1. First/Last Day Churner"
ELSE "2. Other Date Churner" END AS ChurnDateType,
CASE WHEN LastOverdueRecord >= 90 THEN "2.Fixed Mixed Churner"
ELSE "1.Fixed Voluntary Churner" END AS ChurnerType
FROM FIXEDCHURNEDRGUS f INNER JOIN RGUSLastRecordDNA l ON f.src_account_id = l.act_acct_cd 
AND f.NumChurns = l.NumRgusLastRecord AND f.ChurnMonth = l.Month
INNER JOIN LastRecordDateDNA d on f.src_account_id = d.act_acct_cd AND f.ChurnMonth = d.Month
INNER JOIN OverdueLastRecordDNA o ON f.src_account_id = o.act_acct_cd AND f.ChurnMonth = o.Month
)
,VoluntaryChurners AS(
SELECT Month, SAFE_CAST(act_acct_cd AS STRING) AS Account, ChurnerType, ChurnTenureDays
FROM VoluntaryTotalChurners 
WHERE ChurnerType="1.Fixed Voluntary Churner"
GROUP BY Month, act_acct_cd, ChurnerType, ChurnTenureDays
)
---------------------------------------------------Involuntary Churners-------------------------------------------------------------------
,CUSTOMERS_FIRSTLAST_RECORD AS(
 SELECT DISTINCT DATE_TRUNC (dt, MONTH) AS MES, act_acct_cd AS Account, Min(dt) as FirstCustRecord, Max(dt) as LastCustRecord
 FROM UsefulFields
 GROUP BY MES, account
),
NO_OVERDUE AS(
 SELECT DISTINCT DATE_TRUNC (dt, MONTH) AS MES, act_acct_cd AS Account, fi_outst_age
 FROM UsefulFields t
 INNER JOIN CUSTOMERS_FIRSTLAST_RECORD r ON t.dt = r.FirstCustRecord and r.account = t.act_acct_cd
 WHERE fi_outst_age <= 90
 GROUP BY MES, account, fi_outst_age
),
OVERDUELASTDAY AS(
 SELECT DISTINCT DATE_TRUNC (dt, MONTH) AS MES, act_acct_cd AS Account, fi_outst_age,
 (date_diff(dt, MaxStart, DAY)) as ChurnTenureDays
 FROM UsefulFields t
 INNER JOIN CUSTOMERS_FIRSTLAST_RECORD r ON t.dt = r.LastCustRecord and r.account = t.act_acct_cd
 WHERE  fi_outst_age >= 90
 GROUP BY MES, account, fi_outst_age, ChurnTenureDays
),
INVOLUNTARYNETCHURNERS AS(
 SELECT DISTINCT n.MES AS Month, n. account, l.ChurnTenureDays
 FROM NO_OVERDUE n INNER JOIN OVERDUELASTDAY l ON n.account = l.account and n.MES = l.MES
)
,InvoluntaryChurners AS(
SELECT DISTINCT Month, SAFE_CAST(Account AS STRING) AS Account, ChurnTenureDays
,CASE WHEN Account IS NOT NULL THEN "2. Fixed Involuntary Churner" END AS ChurnerType
FROM INVOLUNTARYNETCHURNERS 
GROUP BY Month, Account,ChurnerType, ChurnTenureDays
)
,AllChurners AS(
SELECT DISTINCT Month,Account,ChurnerType, ChurnTenureDays
from (SELECT Month,Account,ChurnerType, ChurnTenureDays from VoluntaryChurners a 
      UNION ALL
      SELECT Month,Account,ChurnerType, ChurnTenureDays from InvoluntaryChurners b)
),

FullFixedBase AS(
SELECT s.* ,
CASE WHEN c.account IS NOT NULL THEN "1. Fixed Churner"
ELSE "2. Non-churner" END AS FixedChurnFlag,
case WHEN c.account IS NOT NULL THEN ChurnerType
ELSE "2.Non-Churners" END AS FixedChurnTypeFlag,
ChurnTenureDays, CASE WHEN ChurnTenureDays <= 180 Then "0.Early-tenure Churner"
WHEN ChurnTenureDays > 180 THEN "1.Late-tenure Churner"
WHEN ChurnTenureDays IS NULL then "2.Non-Churner"
END AS ChurnTenureSegment
FROM SPINMOVEMENTBASE s LEFT JOIN AllChurners c ON s.Fixed_Account = SAFE_CAST(c.Account AS Numeric) AND s.Fixed_Month = c.Month
)

,InactiveUsers AS (
SELECT DISTINCT Fixed_Month AS ExitMonth, fixed_account 
FROM FullFixedBase
WHERE ActiveBOM=1 AND ActiveEOM=0
)

,FullFixedBase AS(
SELECT f.*, ExitMonth
FROM FullFixedBase f LEFT JOIN InactiveUsers i ON f.Fixed_account=i.Fixed_account AND fixed_month=ExitMonth
)

,RejoinerPopulation AS(
    SELECT CASE
    WHEN 

)

