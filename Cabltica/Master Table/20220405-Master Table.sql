WITH CR_UsefulFields AS(

SELECT DISTINCT DATE_TRUNC (FECHA_EXTRACCION, MONTH) AS Month,FECHA_EXTRACCION, act_acct_cd, pd_vo_prod_id, pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE, Max(ACT_ACCT_INST_DT) as MaxInst,
CASE WHEN pd_vo_prod_id IS NOT NULL THEN 1 ELSE 0 END AS RGU_VO,
CASE WHEN pd_tv_prod_cd IS NOT NULL THEN 1 ELSE 0 END AS RGU_TV,
CASE WHEN pd_bb_prod_id IS NOT NULL THEN 1 ELSE 0 END AS RGU_BB,
CASE WHEN DATE_TRUNC (FECHA_EXTRACCION, MONTH) < '2022-02-01' THEN LAST_DAY (FECHA_EXTRACCION, MONTH)
WHEN DATE_TRUNC (FECHA_EXTRACCION, MONTH) = '2022-02-01' THEN '2022-02-27' END AS Month_Last_Day,
CASE 
WHEN PD_VO_PROD_ID IS NOT NULL AND PD_BB_PROD_NM IS NOT NULL AND PD_TV_PROD_CD IS NOT NULL THEN "3P"
WHEN PD_VO_PROD_ID IS NULL AND PD_BB_PROD_NM IS NOT NULL AND PD_TV_PROD_CD IS NOT NULL THEN "2P"
WHEN PD_VO_PROD_ID IS NOT NULL AND PD_BB_PROD_NM IS NULL AND PD_TV_PROD_CD IS NOT NULL THEN"2P"
WHEN PD_VO_PROD_ID IS NOT NULL AND PD_BB_PROD_NM IS NOT NULL AND PD_TV_PROD_CD IS NULL THEN"2P"
ELSE "1P" END AS MIX
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-02-16_FINAL_HISTORIC_CRM_FILE_2021_D`
GROUP BY Month, FECHA_EXTRACCION, ACT_ACCT_cd, PD_VO_PROD_ID,pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE
)

,CustomerBase_BOM AS(

    SELECT DISTINCT DATE_TRUNC(DATE_ADD(Fecha_Extraccion, INTERVAL 1 MONTH),MONTH) AS Month, Fecha_Extraccion AS B_DATE, act_acct_cd AS AccountBOM, pd_vo_prod_id as B_VO_id, pd_vo_prod_nm as B_VO_nm, pd_tv_prod_id AS B_TV_id, 
    pd_tv_prod_cd as B_TV_nm, pd_bb_prod_id as B_BB_id, pd_bb_prod_nm as B_BB_nm, RGU_VO as B_RGU_VO, RGU_TV as B_RGU_TV, RGU_BB AS B_RGU_BB, fi_outst_age as B_Overdue, C_CUST_AGE as B_Tenure, MaxInst as B_MaxInst, MIX AS B_MIX,
    RGU_VO + RGU_TV + RGU_BB AS B_NumRGUs, Tipo_Tecnologia AS B_Tech_Type,
    CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN "1P"
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) OR (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN "2P"
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN "3P" END AS B_Bundle_Type,
    CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) THEN "VO"
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) THEN "TV"
    WHEN (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN "BB"
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) THEN "TV+VO"
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) THEN "BB+TV"
    WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN "BB+VO"
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN "BB+TV+VO" END AS B_BundleName
    FROM CR_UsefulFields LEFT JOIN `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-13_CR_CATALOGUE_TV_INTERNET_2021_T` ON PD_BB_PROD_nm=ActivoInternet
    WHERE Fecha_extraccion = Month_Last_Day
)

,FinalCustomerBase_BOM AS(
    SELECT *,
    CASE 
    WHEN B_Tech_Type IS NOT NULL THEN B_Tech_Type
    WHEN B_Tech_Type IS NULL AND safe_cast(B_RGU_TV AS string)="NextGenTV" THEN "FTTH"
    ELSE "HFC" END AS B_TechAdj,
    CASE
    WHEN B_Tenure <=6 THEN "EARLY TENURE"
    WHEN B_Tenure >6 THEN "LATE TENURE"
    ELSE NULL END AS B_TenureType
    FROM CustomerBase_BOM 
)


,CustomerBase_EOM AS(
    
    SELECT DISTINCT DATE_TRUNC(Fecha_Extraccion,MONTH) AS Month, Fecha_Extraccion as E_Date, act_acct_cd as AccountEOM, pd_vo_prod_id as E_VO_id, pd_vo_prod_nm as E_VO_nm, pd_tv_prod_cd AS E_TV_id, 
    pd_tv_prod_cd as E_TV_nm, pd_bb_prod_id as E_BB_id, pd_bb_prod_nm as E_BB_nm, RGU_VO as E_RGU_VO, RGU_TV as E_RGU_TV, RGU_BB AS E_RGU_BB, fi_outst_age as E_Overdue, C_CUST_AGE as E_Tenure, MaxInst as E_MaxInst, MIX AS E_MIX,
    RGU_VO + RGU_TV + RGU_BB AS E_NumRGUs,Tipo_Tecnologia AS E_Tech_Type,
    CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN "1P"
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) OR (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN "2P"
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN "3P" END AS E_Bundle_Type,
    CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) THEN "VO"
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) THEN "TV"
    WHEN (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN "BB"
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) THEN "TV+VO"
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) THEN "BB+TV"
    WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN "BB+VO"
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN "BB+TV+VO" END AS E_BundleName
    FROM CR_UsefulFields LEFT JOIN `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-13_CR_CATALOGUE_TV_INTERNET_2021_T` ON PD_BB_PROD_nm=ActivoInternet
    WHERE Fecha_extraccion = Month_Last_Day
)

,FinalCustomerBase_EOM AS(
    SELECT *,
    CASE 
    WHEN E_Tech_Type IS NOT NULL THEN E_Tech_Type
    WHEN E_Tech_Type IS NULL AND safe_cast(E_RGU_TV AS string)="NextGenTV" THEN "FTTH"
    ELSE "HFC" END AS E_TechAdj, CASE
    WHEN E_Tenure <=6 THEN "EARLY TENURE"
    WHEN E_Tenure >6 THEN "LATE TENURE"
    ELSE NULL END AS E_TenureType

    FROM CustomerBase_EOM 
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
   B_Date, B_VO_id, B_VO_nm, B_TV_id, B_TV_nm, B_BB_id, B_BB_nm, B_RGU_VO, B_RGU_TV, B_RGU_BB, B_NumRGUs, B_Overdue, B_Tenure, B_MaxInst, B_Bundle_Type, B_BundleName,B_MIX, B_TechAdj,B_TenureType,
   E_Date, E_VO_id, E_VO_nm, E_TV_id, E_TV_nm, E_BB_id, E_BB_nm, E_RGU_VO, E_RGU_TV, E_RGU_BB, E_NumRGUs, E_Overdue, E_Tenure, E_MaxInst, E_Bundle_Type, E_BundleName,E_MIX, E_TechAdj,E_TenureType,
  FROM FinalCustomerBase_BOM b FULL OUTER JOIN FinalCustomerBase_EOM e ON b.AccountBOM = e.AccountEOM AND b.Month = e.Month
)

--,FixedCustomerBase_MvmtFlag AS(

 SELECT f.*,
 CASE WHEN E_NumRGUs > B_NumRGUs THEN "Upsell"
 WHEN E_NumRGUs < B_NumRGUs THEN "Downsell"
 WHEN E_NumRGUs = B_NumRGUs THEN "Same RGUs"
 WHEN ActiveBOM = 0 AND ACTIVEEOM = 1 THEN "Gain_GrossAds"
 WHEN ActiveBOM = 1 AND ActiveEOM = 0 THEN "Loss"
 END AS MainMovement,
 CASE WHEN ActiveBOM = 0 AND ActiveEOM = 1 AND DATE_TRUNC(E_MaxInst,Month) = "2022-02-01" THEN "Feb Gross-Ads"
 WHEN ActiveBOM = 0 AND ActiveEOM = 1 AND DATE_TRUNC(E_MaxInst,Month) <> "2022-02-01" THEN "ComeBackToLife/Rejoiners Gross-Ads"
 ELSE NULL END AS GainMovement,
 IFNULL(E_RGU_BB - B_RGU_BB,0) as DIF_RGU_BB , IFNULL(E_RGU_TV - B_RGU_TV,0) as DIF_RGU_TV , IFNULL(E_RGU_VO - B_RGU_VO,0) as DIF_RGU_VO ,
 (E_NumRGUs - B_NumRGUs) as DIF_TOTAL_RGU
 FROM FixedCustomerBase f

--)
--------------------------------------Opening/Closing base KPIs--------------------------------------------------
-- Opening/Closing Customer Base
/*SELECT Fixed_Month, SUM(ActiveBOM) as OpeningBase, Sum(ActiveEOM) as ClosingBase
FROM FixedCustomerBase_MvmtFlag
GROUP BY Fixed_Month
ORDER BY Fixed_Month*/

-- Opening/Closing RGU Base
/*SELECT Fixed_Month, SUM(B_RGU_VO) AS OPENING_VO, SUM(B_RGU_TV) AS OPENING_TV, SUM (B_RGU_BB) AS OPENING_BB, SUM(B_NumRGUs) as Opening_TotalRGUs,
SUM(E_RGU_VO) AS CLOSING_VO, SUM(E_RGU_TV) AS CLOSING_TV, SUM (E_RGU_BB) AS CLOSING_BB, SUM(E_NumRGUs) as Closing_TotalRGUs
FROM FixedCustomerBase_MvmtFlag
GROUP BY Fixed_Month
ORDER BY Fixed_Month*/

 --Customer Movements KPI
/*SELECT Fixed_Month, MainMovement, GainMovement, Count(DISTINCT Fixed_Account) NumCustomers
FROM FixedCustomerBase_MvmtFlag
WHERE Fixed_Month = '2022-02-01'
GROUP BY Fixed_Month, MainMovement, GainMovement
ORDER BY Fixed_Month, MainMovement, GainMovement*/

-- RGU Gross Ads
/*SELECT Fixed_Month, MainMovement, GainMovement, Sum (E_RGU_VO) as GrossAds_VO, SUM (E_RGU_TV) as GrossAds_TV, Sum(E_RGU_BB) as GrossAds_BB, SUM (E_NumRGUs) as TotalRGUsGrossAds
FROM FixedCustomerBase_MvmtFlag
WHERE Fixed_Month = '2022-02-01' AND MainMovement = "Gain_GrossAds"
GROUP BY Fixed_Month, MainMovement, GainMovement
ORDER BY Fixed_Month, MainMovement, GainMovement*/

/*SELECT Fixed_Month, MainMovement, SUM(DIF_RGU_VO) AS ChangeVO,SUM(DIF_RGU_TV) AS ChangeTV, SUM(DIF_RGU_BB) AS ChangeBB, SUM (DIF_TOTAL_RGU) AS ChangeRGUs
FROM FixedCustomerBase_MvmtFlag
WHERE Fixed_Month = '2022-02-01' AND (MainMovement = "Upsell" OR MainMovement = "Downsell")
GROUP BY Fixed_Month, MainMovement, GainMovement
ORDER BY Fixed_Month, MainMovement, GainMovement*/


