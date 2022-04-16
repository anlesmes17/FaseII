WITH 
CR_UsefulFields_BOM AS(

SELECT DISTINCT DATE_TRUNC (FECHA_EXTRACCION, MONTH) AS Month,FECHA_EXTRACCION, act_acct_cd, pd_vo_prod_id, pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE, Max(ACT_ACCT_INST_DT) as MaxInst, CST_CHRN_DT AS ChurnDate, DATE_DIFF(FECHA_EXTRACCION, OLDEST_UNPAID_BILL_DT,DAY) AS MORA,
VO_FI_TOT_MRC_AMT, BB_FI_TOT_MRC_AMT, TV_FI_TOT_MRC_AMT,
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
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE,CST_CHRN_DT, OLDEST_UNPAID_BILL_DT, VO_FI_TOT_MRC_AMT, BB_FI_TOT_MRC_AMT, TV_FI_TOT_MRC_AMT
)

,CR_UsefulFields_EOM AS(

SELECT DISTINCT DATE_TRUNC (FECHA_EXTRACCION, MONTH) AS Month,FECHA_EXTRACCION, act_acct_cd, pd_vo_prod_id, pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, VO_FI_TOT_MRC_AMT, BB_FI_TOT_MRC_AMT, TV_FI_TOT_MRC_AMT, FI_OUTST_AGE, C_CUST_AGE, Max(ACT_ACCT_INST_DT) as MaxInst, CST_CHRN_DT AS ChurnDate,  DATE_DIFF(FECHA_EXTRACCION, OLDEST_UNPAID_BILL_DT,DAY) AS MORA,
CASE WHEN pd_vo_prod_id IS NOT NULL THEN 1 ELSE 0 END AS RGU_VO,
CASE WHEN pd_tv_prod_cd IS NOT NULL THEN 1 ELSE 0 END AS RGU_TV,
CASE WHEN pd_bb_prod_id IS NOT NULL THEN 1 ELSE 0 END AS RGU_BB,
CASE WHEN DATE_TRUNC (FECHA_EXTRACCION, MONTH) < '2022-03-01' THEN LAST_DAY (FECHA_EXTRACCION, MONTH)
END AS Month_Last_Day,
CASE 
WHEN PD_VO_PROD_ID IS NOT NULL AND PD_BB_PROD_NM IS NOT NULL AND PD_TV_PROD_CD IS NOT NULL THEN "3P"
WHEN PD_VO_PROD_ID IS NULL AND PD_BB_PROD_NM IS NOT NULL AND PD_TV_PROD_CD IS NOT NULL THEN "2P"
WHEN PD_VO_PROD_ID IS NOT NULL AND PD_BB_PROD_NM IS NULL AND PD_TV_PROD_CD IS NOT NULL THEN"2P"

WHEN PD_VO_PROD_ID IS NOT NULL AND PD_BB_PROD_NM IS NOT NULL AND PD_TV_PROD_CD IS NULL THEN"2P"
ELSE "1P" END AS MIX
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220405_CRM_march_sample`
GROUP BY Month, FECHA_EXTRACCION, ACT_ACCT_cd, PD_VO_PROD_ID,pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE,CST_CHRN_DT, OLDEST_UNPAID_BILL_DT, VO_FI_TOT_MRC_AMT, BB_FI_TOT_MRC_AMT, TV_FI_TOT_MRC_AMT
)

,CR_UsefulFields AS(
SELECT  DISTINCT *
from (SELECT Month,FECHA_EXTRACCION, act_acct_cd, pd_vo_prod_id, pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE,MaxInst, ChurnDate, RGU_VO,RGU_BB, RGU_TV,
Month_Last_Day, MIX, MORA, VO_FI_TOT_MRC_AMT, BB_FI_TOT_MRC_AMT, TV_FI_TOT_MRC_AMT from CR_UsefulFields_BOM b 
      UNION ALL
      SELECT Month,FECHA_EXTRACCION, act_acct_cd, pd_vo_prod_id, pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE,MaxInst, ChurnDate, RGU_VO,RGU_BB, RGU_TV,
Month_Last_Day, MIX, MORA, VO_FI_TOT_MRC_AMT, BB_FI_TOT_MRC_AMT, TV_FI_TOT_MRC_AMT from CR_UsefulFields_EOM e)

)
,AverageMRC_User AS(
  SELECT DISTINCT DATE_TRUNC(DATE(FECHA_EXTRACCION),MONTH) AS Month, act_acct_cd, avg(VO_FI_TOT_MRC_AMT + BB_FI_TOT_MRC_AMT + TV_FI_TOT_MRC_AMT) AS AvgMRC
  FROM CR_UsefulFields_BOM
  GROUP BY Month, act_acct_cd
)

,CustomerBase_BOM AS(

    SELECT DISTINCT DATE_TRUNC(DATE_ADD(Fecha_Extraccion, INTERVAL 1 MONTH),MONTH) AS Month, Fecha_Extraccion AS B_DATE, act_acct_cd AS AccountBOM, pd_vo_prod_id as B_VO_id, pd_vo_prod_nm as B_VO_nm, pd_tv_prod_id AS B_TV_id, 
    pd_tv_prod_cd as B_TV_nm, pd_bb_prod_id as B_BB_id, pd_bb_prod_nm as B_BB_nm, RGU_VO as B_RGU_VO, RGU_TV as B_RGU_TV, RGU_BB AS B_RGU_BB, fi_outst_age as B_Overdue, C_CUST_AGE as B_Tenure, MaxInst as B_MaxInst, MIX AS B_MIX,
    RGU_VO + RGU_TV + RGU_BB AS B_NumRGUs, Tipo_Tecnologia AS B_Tech_Type, MORA AS B_MORA, VO_FI_TOT_MRC_AMT as B_VO_MRC, BB_FI_TOT_MRC_AMT as B_BB_MRC, TV_FI_TOT_MRC_AMT as B_TV_MRC,
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
    WHERE safe_cast(Fecha_extraccion as string) = safe_cast(Month_Last_Day as string)
)

,FinalCustomerBase_BOM AS(
    SELECT *,
    CASE 
    WHEN B_Tech_Type IS NOT NULL THEN B_Tech_Type
    WHEN B_Tech_Type IS NULL AND safe_cast(B_RGU_TV AS string)="NextGenTV" THEN "FTTH"
    ELSE "HFC" END AS B_TechAdj,
    CASE
    WHEN B_Tenure <6 THEN "Early Tenure"
    WHEN B_Tenure >=6 THEN "Late Tenure"
    ELSE NULL END AS B_TenureType
    FROM CustomerBase_BOM 
)

,CustomerBase_EOM AS(
    
    SELECT DISTINCT DATE_TRUNC(Fecha_Extraccion,MONTH) AS Month, Fecha_Extraccion as E_Date, act_acct_cd as AccountEOM, pd_vo_prod_id as E_VO_id, pd_vo_prod_nm as E_VO_nm, pd_tv_prod_cd AS E_TV_id, 
    pd_tv_prod_cd as E_TV_nm, pd_bb_prod_id as E_BB_id, pd_bb_prod_nm as E_BB_nm, RGU_VO as E_RGU_VO, RGU_TV as E_RGU_TV, RGU_BB AS E_RGU_BB, fi_outst_age as E_Overdue, C_CUST_AGE as E_Tenure, MaxInst as E_MaxInst, MIX AS E_MIX,
    RGU_VO + RGU_TV + RGU_BB AS E_NumRGUs,Tipo_Tecnologia AS E_Tech_Type, MORA AS E_MORA,VO_FI_TOT_MRC_AMT AS E_VO_MRC, BB_FI_TOT_MRC_AMT, TV_FI_TOT_MRC_AMT,
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
    WHEN E_Tenure <6 THEN "EARLY TENURE"
    WHEN E_Tenure >=6 THEN "LATE TENURE"
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
   B_Date, B_VO_id, B_VO_nm, B_TV_id, B_TV_nm, B_BB_id, B_BB_nm, B_RGU_VO, B_RGU_TV, B_RGU_BB, B_NumRGUs, B_Overdue, B_Tenure, B_MaxInst, B_Bundle_Type, B_BundleName,B_MIX, B_TechAdj,B_TenureType, B_MORA,
   E_Date, E_VO_id, E_VO_nm, E_TV_id, E_TV_nm, E_BB_id, E_BB_nm, E_RGU_VO, E_RGU_TV, E_RGU_BB, E_NumRGUs, E_Overdue, E_Tenure, E_MaxInst, E_Bundle_Type, E_BundleName,E_MIX, E_TechAdj,E_TenureType, E_MORA,
  FROM FinalCustomerBase_BOM b FULL OUTER JOIN FinalCustomerBase_EOM e ON b.AccountBOM = e.AccountEOM AND b.Month = e.Month
)

,FixedCustomerBase_MvmtFlag AS(
 SELECT f.*,
 CASE WHEN E_NumRGUs > B_NumRGUs THEN "Upsell"
 WHEN E_NumRGUs < B_NumRGUs THEN "Downsell"
 WHEN E_NumRGUs = B_NumRGUs THEN "Same RGUs"--Incluir Info MRC
 WHEN ActiveBOM = 0 AND ACTIVEEOM = 1 THEN "Gain_GrossAds"
 WHEN ActiveBOM = 1 AND ActiveEOM = 0 THEN "Loss"
 END AS MainMovement,
 CASE WHEN ActiveBOM = 0 AND ActiveEOM = 1 AND DATE_TRUNC(E_MaxInst,Month) = "2022-02-01" THEN "Feb Gross-Ads"
 WHEN ActiveBOM = 0 AND ActiveEOM = 1 AND DATE_TRUNC(E_MaxInst,Month) <> "2022-02-01" THEN "ComeBackToLife/Rejoiners Gross-Ads"
 ELSE NULL END AS GainMovement,
 IFNULL(E_RGU_BB - B_RGU_BB,0) as DIF_RGU_BB , IFNULL(E_RGU_TV - B_RGU_TV,0) as DIF_RGU_TV , IFNULL(E_RGU_VO - B_RGU_VO,0) as DIF_RGU_VO ,
 (E_NumRGUs - B_NumRGUs) as DIF_TOTAL_RGU
 FROM FixedCustomerBase f


)
,MobileUsefulFields_BOM AS (
    SELECT DISTINCT Date_Trunc(FECHA_PARQUE, month) AS Month, Contrato, ID_CLIENTE, NUM_IDENT, ID_ABONADO, AGRUPACION_COMUNIDAD
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220411_cabletica_fmc_febrero` 
    WHERE DES_SEGMENTO_CLIENTE <>"Empresas - Empresas" AND DES_SEGMENTO_CLIENTE <>"Empresas - Pymes" --AND (DES_USO ="MOVIL CONTRATO" OR DES_USO="DATOS MOVIL POSPAGO (UMTS/DATACARD)" OR DES_USO="DATOS MOVIL POSPAGO (UMTS/DATACARD)") AND (AGRUPACION_COMUNIDAD<>"NULL" AND AGRUPACION_COMUNIDAD<>"OTROS" AND AGRUPACION_COMUNIDAD<>"B2H")
)
,MobileUsefulFields_EOM AS (
    SELECT DISTINCT Date_Trunc(FECHA_PARQUE, month) AS Month,Contrato, ID_CLIENTE, NUM_IDENT, ID_ABONADO, AGRUPACION_COMUNIDAD
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220411_cabletica_fmc_marzo`
    WHERE DES_SEGMENTO_CLIENTE <>"Empresas - Empresas" AND DES_SEGMENTO_CLIENTE <>"Empresas - Pymes" --AND (DES_USO ="MOVIL CONTRATO" OR DES_USO="DATOS MOVIL POSPAGO (UMTS/DATACARD)" OR DES_USO="DATOS MOVIL POSPAGO (UMTS/DATACARD)") AND (AGRUPACION_COMUNIDAD<>"NULL" AND AGRUPACION_COMUNIDAD<>"OTROS" AND AGRUPACION_COMUNIDAD<>"B2H")
)
,CR_Mobile_UsefulFields AS(
SELECT  DISTINCT *
from (SELECT * from MobileUsefulFields_BOM b 
      UNION ALL
      SELECT * from MobileUsefulFields_EOM e)
)
,BaseRentaTenure AS (
    SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220411_cabletica_tenure_renta_mobile`
)
,BaseMovimientos AS(
    SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220404_cabletica_movimientos_fmc`
)



,MobileCustomerBase_BOM AS(
    SELECT DISTINCT  DATE_TRUNC(Month,Month) AS Month, ID_ABONADO AS B_Mobile_Account, Contrato as B_Contrato
    from CR_Mobile_UsefulFields 
)

,MobileCustomerBase_EOM AS(
    SELECT DISTINCT  DATE_TRUNC(DATE_SUB(Month, INTERVAL 1 MONTH),MONTH) AS Month, ID_ABONADO AS E_Mobile_Account, Contrato as E_Contrato
    from CR_Mobile_UsefulFields 
)
,MobileCustomerBase AS (
    SELECT DISTINCT
    CASE WHEN (B_Mobile_Account IS NOT NULL AND E_Mobile_Account IS NOT NULL) OR (B_Mobile_Account IS NOT NULL AND E_Mobile_Account IS NULL) THEN b.Month
    WHEN (B_Mobile_Account IS NULL AND E_Mobile_Account IS NOT NULL) THEN e.Month
    END AS Mobile_Month,
    CASE WHEN (B_Mobile_Account IS NOT NULL AND E_Mobile_Account IS NOT NULL) OR (B_Mobile_Account IS NOT NULL AND E_Mobile_Account IS NULL) THEN B_Mobile_Account
    WHEN (B_Mobile_Account IS NULL AND E_Mobile_Account IS NOT NULL) THEN E_Mobile_Account
    END AS Mobile_Account,
    CASE WHEN B_Mobile_Account IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM,
    CASE WHEN E_Mobile_Account IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM,
    B_Contrato,
    E_Contrato
    FROM MobileCustomerBase_BOM b FULL OUTER JOIN MobileCustomerBase_EOM e ON B_Mobile_Account=E_Mobile_Account AND b.Month=e.Month
 
)
SELECT DISTINCT MOBILE_MONTH, COUNT(DISTINCT Mobile_Account) FROM MobileCustomerBase
WHERE ActiveBOM=1 AND ActiveEOM=1
GROUP BY MOBILE_Month


/*,FixedCustomerBase AS(
    SELECT DISTINCT
    CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
   END AS Fixed_Month,
     CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
      WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
  END AS Fixed_Account,
   CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM,
   CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM,
   B_Date, B_VO_id, B_VO_nm, B_TV_id, B_TV_nm, B_BB_id, B_BB_nm, B_RGU_VO, B_RGU_TV, B_RGU_BB, B_NumRGUs, B_Overdue, B_Tenure, B_MaxInst, B_Bundle_Type, B_BundleName,B_MIX, B_TechAdj,B_TenureType, B_MORA,
   E_Date, E_VO_id, E_VO_nm, E_TV_id, E_TV_nm, E_BB_id, E_BB_nm, E_RGU_VO, E_RGU_TV, E_RGU_BB, E_NumRGUs, E_Overdue, E_Tenure, E_MaxInst, E_Bundle_Type, E_BundleName,E_MIX, E_TechAdj,E_TenureType, E_MORA,
  FROM FinalCustomerBase_BOM b FULL OUTER JOIN FinalCustomerBase_EOM e ON b.AccountBOM = e.AccountEOM AND b.Month = e.Month
)*/




--------------------------------------Opening/Closing base KPIs--------------------------------------------------
--,Opening/Closing Customer Base
/*SELECT Fixed_Month, SUM(ActiveBOM) as OpeningBase, Sum(ActiveEOM) as ClosingBase
FROM FixedCustomerBase_MvmtFlag
GROUP BY Fixed_Month
ORDER BY Fixed_Month*/
--)


-- Opening/Closing RGU Base
/*SELECT Fixed_Month, SUM(B_RGU_VO) AS OPENING_VO, SUM(B_RGU_TV) AS OPENING_TV, SUM (B_RGU_BB) AS OPENING_BB, SUM(B_NumRGUs) as Opening_TotalRGUs,
SUM(E_RGU_VO) AS CLOSING_VO, SUM(E_RGU_TV) AS CLOSING_TV, SUM (E_RGU_BB) AS CLOSING_BB, SUM(E_NumRGUs) as Closing_TotalRGUs
FROM FixedCustomerBase_MvmtFlag
GROUP BY Fixed_Month
ORDER BY Fixed_Month*/

/*,Customer Movements KPI
SELECT Fixed_Month, MainMovement, GainMovement, Count(DISTINCT Fixed_Account) NumCustomers
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

--------------------------------------------Churn---------------------------------------------------------------

,CHURNERSCRM AS(
    SELECT  DISTINCT ACT_ACCT_CD, MAX(ChurnDate) AS Maxfecha,  DATE_TRUNC(Max(ChurnDate),month) AS MesChurnF, 
    FROM  CR_UsefulFields
    GROUP BY ACT_ACCT_CD
    HAVING DATE_TRUNC(safe_cast(Maxfecha as date), MONTH) = DATE_TRUNC(MAX(FECHA_EXTRACCION),MONTH)
)

,MoraChurners AS (
    SELECT DISTINCT ACT_ACCT_CD, FECHA_EXTRACCION, MORA
    FROM CR_UsefulFields 
    GROUP BY ACT_ACCT_CD, MORA, FECHA_EXTRACCION
)

,BaseChurners AS (
SELECT DISTINCT c.ACT_ACCT_CD Account_Churners,m.act_acct_cd, c.Maxfecha, Mora,
FROM CHURNERSCRM c LEFT JOIN MoraChurners m ON c.act_acct_cd=m.act_acct_cd AND safe_cast(c.Maxfecha as date)=safe_cast(m.FECHA_EXTRACCION as date)
)

,ChurnersVolInvol as(
SELECT *,
CASE 
WHEN MORA<90 OR MORA IS NULL THEN "Voluntary"
WHEN MORA>=90 THEN "Involuntary"
ELSE NULL END AS ChurnType
FROM BaseChurners
)
,MasterTableChurners AS (
    SELECT m.*, ChurnType FROM FixedCustomerBase_MvmtFlag m LEFT JOIN ChurnersVolInvol 
    ON Fixed_Account=Account_Churners AND DATE_TRUNC(safe_cast(MaxFecha as date),Month)=Fixed_Month
)

/*
SELECT DISTINCT DATE_TRUNC(Maxfecha, month) AS Month, ChurnType, count(*)
FROM ChurnersVolInvol 
GROUP BY Month, ChurnType
ORDER BY Month, ChurnType desc
----------------------------------------------------Mobile----------------------------------------------------
*/


----------------------------------Mobile Opening/ Closing Base & Tenure-------------------------------------------

/*SELECT Month, Count(Distinct ID_ABONADO)  FROM MobileUsefulFields_EOM 
GROUP BY Month*/

,ClosingBaseWithTenure AS(
SELECT DISTINCT Month, ID_CLIENTE, ID_ABONADO 
FROM MobileUsefulFields_EOM 
GROUP BY Month, ID_CLIENTE, ID_ABONADO
)

,TenureClosingBase AS (
    SELECT DISTINCT ID_ABONADO, MIN(MESES_ANTIGUEDAD) AS MESES_ANTIGUEDAD, RENTA
    FROM ClosingBaseWithTenure LEFT JOIN BaseRentaTenure 
    ON ID_ABONADO=NUM_ABONADO
    GROUP BY ID_ABONADO, RENTA
)

,FlagTenureClosingBase AS (
SELECT DISTINCT ID_ABONADO, MESES_ANTIGUEDAD,RENTA,
CASE WHEN MESES_ANTIGUEDAD <6 THEN "Early Tenure"
WHEN MESES_ANTIGUEDAD >=6 THEN "Late Tenure"
ELSE NULL END AS TenureClosingBase
FROM TenureClosingBase
)

--,TenureMobileBase AS (
    SELECT f.*,t.MESES_ANTIGUEDAD, t.TenureClosingBase, renta
    FROM MobileCustomerBase f LEFT JOIN FlagTenureClosingBase t ON f.Mobile_Account=t.ID_ABONADO
)
/*SELECT TenureClosingBase, count(DISTINCT ID_ABONADO)
FROM FlagTenureClosingBase 
GROUP BY TenureClosingBase*/


/*SELECT Month, Count(Distinct ID_CLIENTE) NumClientes FROM MobileUsefulFields_EOM 
GROUP BY Month*/

----------------------------------------------Churners--------------------------------------------------------

--,MobileChurners AS (
Select DISTINCT e.ID_ABONADO AS Account_EOM, b.ID_ABONADO As Account_BOM
FROM MobileUsefulFields_BOM b LEFT JOIN MobileUsefulFields_EOM e ON e.ID_CLIENTE=b.ID_CLIENTE
WHERE e.ID_CLIENTE IS NULL
)
,TipoChurners AS (
SELECT DISTINCT ID_CLIENTE, TIPO_BAJA
FROM MobileChurners m Left Join BaseMovimientos ON ID_CLIENTE=ID_CLIENTE
)
SELECT TIPO_BAJA, COUNT(DISTINCT ID_CLIENTE)
FROM TipoChurners
GROUP BY TIPO_BAJA




,TenureChurners AS (
    SELECT  ID_BOM, Account_BOM, MESES_ANTIGUEDAD
    FROM MobileChurners LEFT JOIN `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220411_cabletica_tenure_renta_mobile` ON ID_BOM=NUM_ABONADO
)

,EarlyLateChurners AS (
    SELECT CASE
    WHEN MESES_ANTIGUEDAD <6 Then "Early Tenure"
    WHEN MESES_ANTIGUEDAD >=6 Then "Late Tenure"
    ELSE NULL END AS ChurnType
    from TenureChurners 
)/*
SELECT DISTINCT ChurnType, Count(*) AS Churners
FROM EarlyLateChurners 
GROUP BY ChurnType*/



---------------------------------------------Gross Adds--------------------------------------------------------

,GrossAdds AS (
SELECT DISTINCT e.ID_ABONADO AS Account_EOM, b.ID_CLIENTE As Account_BOM, e.ID_ABONADO
FROM MobileUsefulFields_EOM e LEFT JOIN MobileUsefulFields_BOM b ON e.ID_CLIENTE=b.ID_CLIENTE
WHERE b.ID_ABONADO IS NULL
)

SELECT DISTINCT e.Month, e.ID_ABONADO
FROM MobileUsefulFields_EOM e LEFT JOIN MobileUsefulFields_BOM b ON e.ID_ABONADO=b.ID_ABONADO
WHERE b.ID_ABONADO IS NULL

,GrossAddsMRC AS(
    SELECT DISTINCT Account_EOM, renta
    FROM GrossAdds Left Join `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220411_cabletica_tenure_renta_mobile`
    ON ID_ABONADO=NUM_ABONADO
)/*
SELECT SUM(safe_cast(replace(RENTA,".","") as integer)) AS RevenueGrossAdds
FROM GrossAddsMrc*/


------------------------------------------------FMC------------------------------------------------------------

,FMC_feb AS (
SELECT DISTINCT Fixed_Month,Month, Fixed_Account, contrato
FROM MobileUsefulFields_BOM m INNER JOIN FixedCustomerBase_MvmtFlag ON safe_cast(Fixed_Account as string)=m.contrato
AND Fixed_Month=Month
)
,ClientesConvergentes AS(
    SELECT F.*, c.contrato
    FROM FixedCustomerBase_MvmtFlag f LEFT JOIN FMC_feb c ON safe_cast(f.Fixed_Account as string)=safe_cast(c.Contrato as string) 
)
--,Bundles AS (
    SELECT Fixed_Month, E_TECHADJ, E_MIX, Count(*)
    FROM ClientesConvergentes
    WHERE contrato IS NOT NULL AND Fixed_Month="2022-02-01"
    GROUP BY E_TechAdj, E_MIX, Fixed_Month
) 


/*
,FMC_mar AS (
    SELECT DISTINCT Fixed_Month,Month, Fixed_Account, contrato
FROM MobileUsefulFields_EOM m INNER JOIN FixedCustomerBase_MvmtFlag ON safe_cast(Fixed_Account as string)=m.contrato
AND Fixed_Month=Month
)
,FMC_unpacking AS (
    SELECT DISTINCT f.Fixed_Account, m.Fixed_Account
    FROM FMC_feb f LEFT JOIN FMC_mar m ON f.Fixed_Account=m.Fixed_Account
    WHERE m.Fixed_Account is null
)
--,FMC_packing AS (
    SELECT DISTINCT m.Fixed_Account, f.Fixed_Account
    FROM FMC_mar m LEFT JOIN FMC_feb f ON m.Fixed_Account=f.Fixed_Account
    WHERE f.Fixed_Account IS NULL
--)*/
