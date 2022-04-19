CREATE OR REPLACE TABLE

`gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Fixed_DashboardInput` AS

###################################### Fixed Useful Fields #################################################################

WITH 
CR_UsefulFields_BOM AS(

SELECT DISTINCT DATE_TRUNC (safe_cast(FECHA_EXTRACCION as date), MONTH) AS Month,FECHA_EXTRACCION, act_acct_cd, pd_vo_prod_id, pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE, Max(ACT_ACCT_INST_DT) as MaxInst, CST_CHRN_DT AS ChurnDate, DATE_DIFF(safe_cast(FECHA_EXTRACCION as date), safe_cast(OLDEST_UNPAID_BILL_DT as date),DAY) AS MORA,
CASE WHEN pd_vo_prod_id IS NOT NULL THEN 1 ELSE 0 END AS RGU_VO,
CASE WHEN pd_tv_prod_cd IS NOT NULL THEN 1 ELSE 0 END AS RGU_TV,
CASE WHEN pd_bb_prod_id IS NOT NULL THEN 1 ELSE 0 END AS RGU_BB,
CASE WHEN DATE_TRUNC (safe_cast(FECHA_EXTRACCION as date), MONTH) < '2022-02-01' THEN LAST_DAY (safe_cast(FECHA_EXTRACCION as date), MONTH)
WHEN DATE_TRUNC (safe_cast(FECHA_EXTRACCION as date), MONTH) = '2022-02-01' THEN '2022-02-27' END AS Month_Last_Day,
CASE 
WHEN PD_VO_PROD_ID IS NOT NULL AND PD_BB_PROD_NM IS NOT NULL AND PD_TV_PROD_CD IS NOT NULL THEN "3P"
WHEN PD_VO_PROD_ID IS NULL AND PD_BB_PROD_NM IS NOT NULL AND PD_TV_PROD_CD IS NOT NULL THEN "2P"
WHEN PD_VO_PROD_ID IS NOT NULL AND PD_BB_PROD_NM IS NULL AND PD_TV_PROD_CD IS NOT NULL THEN"2P"
WHEN PD_VO_PROD_ID IS NOT NULL AND PD_BB_PROD_NM IS NOT NULL AND PD_TV_PROD_CD IS NULL THEN"2P"
ELSE "1P" END AS MIX
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-02-16_FINAL_HISTORIC_CRM_FILE_2021_D`
GROUP BY Month, FECHA_EXTRACCION, ACT_ACCT_cd, PD_VO_PROD_ID,pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE,CST_CHRN_DT, OLDEST_UNPAID_BILL_DT, VO_FI_TOT_MRC_AMT, BB_FI_TOT_MRC_AMT, TV_FI_TOT_MRC_AMT, VO_FI_TOT_MRC_AMT, BB_FI_TOT_MRC_AMT, TV_FI_TOT_MRC_AMT--, TOT_BILL_AMT
)

,CR_UsefulFields_EOM AS(

SELECT DISTINCT DATE_TRUNC (safe_cast(FECHA_EXTRACCION as date), MONTH) AS Month,FECHA_EXTRACCION, act_acct_cd, pd_vo_prod_id, pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, VO_FI_TOT_MRC_AMT, BB_FI_TOT_MRC_AMT, TV_FI_TOT_MRC_AMT, FI_OUTST_AGE, C_CUST_AGE, Max(ACT_ACCT_INST_DT) as MaxInst, CST_CHRN_DT AS ChurnDate,  DATE_DIFF(safe_cast(FECHA_EXTRACCION as date), safe_cast(OLDEST_UNPAID_BILL_DT as date),DAY) AS MORA,
CASE WHEN pd_vo_prod_id IS NOT NULL THEN 1 ELSE 0 END AS RGU_VO,
CASE WHEN pd_tv_prod_cd IS NOT NULL THEN 1 ELSE 0 END AS RGU_TV,
CASE WHEN pd_bb_prod_id IS NOT NULL THEN 1 ELSE 0 END AS RGU_BB,
CASE WHEN DATE_TRUNC (safe_cast(FECHA_EXTRACCION as date), MONTH) < '2022-03-01' THEN LAST_DAY (safe_cast(FECHA_EXTRACCION as date), MONTH)
END AS Month_Last_Day,
CASE 
WHEN PD_VO_PROD_ID IS NOT NULL AND PD_BB_PROD_NM IS NOT NULL AND PD_TV_PROD_CD IS NOT NULL THEN "3P"
WHEN PD_VO_PROD_ID IS NULL AND PD_BB_PROD_NM IS NOT NULL AND PD_TV_PROD_CD IS NOT NULL THEN "2P"
WHEN PD_VO_PROD_ID IS NOT NULL AND PD_BB_PROD_NM IS NULL AND PD_TV_PROD_CD IS NOT NULL THEN"2P"

WHEN PD_VO_PROD_ID IS NOT NULL AND PD_BB_PROD_NM IS NOT NULL AND PD_TV_PROD_CD IS NULL THEN"2P"
ELSE "1P" END AS MIX
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220405_CRM_march_sample`
GROUP BY Month, FECHA_EXTRACCION, ACT_ACCT_cd, PD_VO_PROD_ID,pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE,CST_CHRN_DT, OLDEST_UNPAID_BILL_DT, VO_FI_TOT_MRC_AMT, BB_FI_TOT_MRC_AMT, TV_FI_TOT_MRC_AMT--, TOT_BILL_AMT
)

,CR_UsefulFields AS(
SELECT  DISTINCT *
from (SELECT Month,FECHA_EXTRACCION, act_acct_cd, pd_vo_prod_id, pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE,MaxInst, ChurnDate, RGU_VO,RGU_BB, RGU_TV,
Month_Last_Day, MIX, MORA,  from CR_UsefulFields_BOM b 
      UNION ALL
      SELECT Month,FECHA_EXTRACCION, act_acct_cd, pd_vo_prod_id, pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE,MaxInst, ChurnDate, RGU_VO,RGU_BB, RGU_TV,
Month_Last_Day, MIX, MORA, from CR_UsefulFields_EOM e)

)
,AverageMRC_User AS(
  SELECT DISTINCT FECHA_EXTRACCION AS Month, act_acct_cd, round(VO_FI_TOT_MRC_AMT,0) AS mrcVO, round(BB_FI_TOT_MRC_AMT,2) AS mrcBB, round(TV_FI_TOT_MRC_AMT,0) AS mrcTV,round((VO_FI_TOT_MRC_AMT + BB_FI_TOT_MRC_AMT + TV_FI_TOT_MRC_AMT),0) as avgmrc, round(TOT_BILL_AMT,0) AS Bill
  FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-18_CRM_Sample_Ene_Mar_2022_Revenue`
  GROUP BY Month, act_acct_cd, mrcVO, mrcBB, mrcTV, Bill, avgmrc
)

,CustomerBase_BOM AS(

    SELECT DISTINCT DATE_TRUNC(DATE_ADD(SAFE_CAST(Fecha_Extraccion AS DATE), INTERVAL 1 MONTH),MONTH) AS Month, Fecha_Extraccion AS B_DATE, c.act_acct_cd AS AccountBOM, pd_vo_prod_id as B_VO_id, pd_vo_prod_nm as B_VO_nm, pd_tv_prod_id AS B_TV_id, 
    pd_tv_prod_cd as B_TV_nm, pd_bb_prod_id as B_BB_id, pd_bb_prod_nm as B_BB_nm, RGU_VO as B_RGU_VO, RGU_TV as B_RGU_TV, RGU_BB AS B_RGU_BB, fi_outst_age as B_Overdue, C_CUST_AGE as B_Tenure, MaxInst as B_MaxInst, MIX AS B_MIX,
    RGU_VO + RGU_TV + RGU_BB AS B_NumRGUs, Tipo_Tecnologia AS B_Tech_Type, MORA AS B_MORA, mrcVO as B_VO_MRC, mrcBB as B_BB_MRC, mrcTV as B_TV_MRC, avgmrc as B_AVG_MRC, BILL AS B_BILL_AMT,
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
    FROM CR_UsefulFields c LEFT JOIN `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-13_CR_CATALOGUE_TV_INTERNET_2021_T` ON PD_BB_PROD_nm=ActivoInternet
    LEFT JOIN AverageMRC_User a ON c.month=a.month AND c.act_acct_cd=a.act_acct_cd
    WHERE safe_cast(Fecha_extraccion as string) = safe_cast(c.Month_Last_Day as string)
)

,FinalCustomerBase_BOM AS(
    SELECT *,
    CASE 
    WHEN B_Tech_Type IS NOT NULL THEN B_Tech_Type
    WHEN B_Tech_Type IS NULL AND safe_cast(B_RGU_TV AS string)="NextGen TV" THEN "FTTH"
    ELSE "HFC" END AS B_TechAdj,
    CASE
    WHEN B_Tenure <=6 THEN "Early Tenure"
    WHEN B_Tenure >6 THEN "Late Tenure"
    ELSE NULL END AS B_TenureType
    FROM CustomerBase_BOM 
)

,CustomerBase_EOM AS(
    
    SELECT DISTINCT DATE_TRUNC(DATE_SUB(Fecha_Extraccion, INTERVAL 1 MONTH), MONTH) AS Month, Fecha_Extraccion as E_Date, c.act_acct_cd as AccountEOM, pd_vo_prod_id as E_VO_id, pd_vo_prod_nm as E_VO_nm, pd_tv_prod_cd AS E_TV_id, 
    pd_tv_prod_cd as E_TV_nm, pd_bb_prod_id as E_BB_id, pd_bb_prod_nm as E_BB_nm, RGU_VO as E_RGU_VO, RGU_TV as E_RGU_TV, RGU_BB AS E_RGU_BB, fi_outst_age as E_Overdue, C_CUST_AGE as E_Tenure, MaxInst as E_MaxInst, MIX AS E_MIX,
    RGU_VO + RGU_TV + RGU_BB AS E_NumRGUs,Tipo_Tecnologia AS E_Tech_Type, MORA AS E_MORA, mrcVO AS E_VO_MRC, mrcBB as E_BB_MRC, mrcTV as E_TV_MRC, avgmrc as E_AVG_MRC, BILL AS E_BILL_AMT,
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
    FROM CR_UsefulFields c LEFT JOIN `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-13_CR_CATALOGUE_TV_INTERNET_2021_T` ON PD_BB_PROD_nm=ActivoInternet
    LEFT JOIN AverageMRC_User a ON c.month=a.month AND c.act_acct_cd=a.act_acct_cd
    WHERE Fecha_extraccion = "2022-03-01"
)

,FinalCustomerBase_EOM AS(
    SELECT *,
    CASE 
    WHEN E_Tech_Type IS NOT NULL THEN E_Tech_Type
    WHEN E_Tech_Type IS NULL AND safe_cast(E_RGU_TV AS string)="NextGen TV" THEN "FTTH"
    ELSE "HFC" END AS E_TechAdj, CASE
    WHEN E_Tenure <=6 THEN "Early Tenure"
    WHEN E_Tenure >6 THEN "Late Tenure"
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
   B_Date, B_VO_id, B_VO_nm, B_TV_id, B_TV_nm, B_BB_id, B_BB_nm, B_RGU_VO, B_RGU_TV, B_RGU_BB, B_NumRGUs, B_Overdue, B_Tenure, B_MaxInst, B_Bundle_Type, B_BundleName,B_MIX, B_TechAdj,B_TenureType, B_MORA, B_VO_MRC, B_BB_MRC, B_TV_MRC, B_AVG_MRC, B_BILL_AMT,
   E_Date, E_VO_id, E_VO_nm, E_TV_id, E_TV_nm, E_BB_id, E_BB_nm, E_RGU_VO, E_RGU_TV, E_RGU_BB, E_NumRGUs, E_Overdue, E_Tenure, E_MaxInst, E_Bundle_Type, E_BundleName,E_MIX, E_TechAdj,E_TenureType, E_MORA, E_VO_MRC, E_BB_MRC, E_TV_MRC, E_AVG_MRC, E_BILL_AMT
  FROM FinalCustomerBase_BOM b FULL OUTER JOIN FinalCustomerBase_EOM e ON b.AccountBOM = e.AccountEOM AND b.Month = e.Month
)

,ServiceOrders AS (
    SELECT * FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-12_CR_ORDENES_SERVICIO_2021-01_A_2021-11_D`
)



###################################################Main Movements################################################################

,MAINMOVEMENTBASE AS(
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
)
,SPINMOVEMENTBASE AS (
    SELECT b.*,
    CASE 
    WHEN MainMovement="Same RGUs" AND (E_BILL_AMT - B_BILL_AMT) > 0 THEN "1. Up-spin" --CAMBIAR ESTO POR TOT_BILL_AMT 
    WHEN MainMovement="Same RGUs" AND (E_BILL_AMT - B_BILL_AMT) < 0 THEN "2. Down-spin" --CAMBIAR ESTO POR TOT_BILL_AMT
    END AS SpinMovement
    FROM MAINMOVEMENTBASE b
)
########################################## Fixed Churn Flags #################################################################

------------------------------------------Voluntary & Involuntary-------------------------------------------------------------

,CHURNERSSO AS
(SELECT DISTINCT RIGHT(CONCAT('0000000000',NOMBRE_CONTRATO) ,10) AS CONTRATOSO, FECHA_APERTURA,
 FROM ServiceOrders 
 WHERE
  TIPO_ORDEN = "DESINSTALACION" 
  AND (ESTADO <> "CANCELADA" OR ESTADO <> "ANULADA")
 AND FECHA_APERTURA IS NOT NULL
 )
,PRIMERCHURNSO AS
(SELECT DISTINCT RIGHT(CONCAT('0000000000',NOMBRE_CONTRATO) ,10) AS CONTRATOSO, Min(FECHA_APERTURA) as PrimerChurnSO,
FROM ServiceOrders
 WHERE
  TIPO_ORDEN = "DESINSTALACION" 
  AND (ESTADO <> "CANCELADA" OR ESTADO <> "ANULADA")
 AND FECHA_APERTURA IS NOT NULL
 GROUP BY CONTRATOSO
 )
,CHURNERSINVOLUNTARIOS AS
(SELECT DISTINCT RIGHT(CONCAT('0000000000',NOMBRE_CONTRATO) ,10) AS CONTRATOSO, FECHA_APERTURA,
 FROM ServiceOrders
 WHERE
  TIPO_ORDEN = "DESINSTALACION" 
  AND (ESTADO <> "CANCELADA" OR ESTADO <> "ANULADA")
  AND SUBMOTIVO = "MOROSIDAD"
 AND FECHA_APERTURA IS NOT NULL
 )
,CHURNTYPEFLAGSO AS(
    SELECT DISTINCT c. CONTRATOSO, c.PrimerChurnSO,
    CASE WHEN t.CONTRATOSO IS NULL THEN "Voluntario"
    WHEN t.CONTRATOSO IS NOT NULL THEN "Involuntario" END AS FixedChurnType
    FROM PRIMERCHURNSO c LEFT JOIN CHURNERSINVOLUNTARIOS t ON c.CONTRATOSO = t.CONTRATOSO AND t.FECHA_APERTURA = c.PrimerChurnSO
)

,CustomerBaseWithChurners AS(
 SELECT DISTINCT c.*, RIGHT(CONCAT('0000000000',Fixed_Account) ,10) AS act_acct_cd
 FROM SPINMOVEMENTBASE c 
)

--CRUCECHURNERSCRM AS(
 SELECT DISTINCT C.*, FixedChurnType
 FROM CustomerBaseWithChurners  c LEFT JOIN CHURNTYPEFLAGSO s ON safe_cast(s.Contratoso as string)= act_acct_cd AND date_trunc(primerchurnSO, month) = Fixed_Month
 WHERE Fixed_Month="2022-02-01" 


/*SELECT Fixed_Month, ChurnType, count(*)
FROM CRUCECHURNERSCRM
GROUP BY 1,2
ORDER BY 1 DESC*/
