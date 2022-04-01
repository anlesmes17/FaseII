WITH CR_UsefulFields AS(

SELECT DISTINCT DATE_TRUNC (FECHA_EXTRACCION, MONTH) AS Month,FECHA_EXTRACCION, act_acct_cd, pd_vo_prod_id, pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE, Max(ACT_ACCT_INST_DT) as MaxInst, CST_CHRN_DT AS ChurnDate,
CASE WHEN pd_vo_prod_id IS NOT NULL THEN 1 ELSE 0 END AS RGU_VO,
CASE WHEN pd_tv_prod_cd IS NOT NULL THEN 1 ELSE 0 END AS RGU_TV,
CASE WHEN pd_bb_prod_id IS NOT NULL THEN 1 ELSE 0 END AS RGU_BB,
CASE WHEN DATE_TRUNC (FECHA_EXTRACCION, MONTH) < '2022-02-01' THEN LAST_DAY (FECHA_EXTRACCION, MONTH)
WHEN DATE_TRUNC (FECHA_EXTRACCION, MONTH) = '2022-02-01' THEN '2022-02-27' END AS Month_Last_Day
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-02-16_FINAL_HISTORIC_CRM_FILE_2021_D`
GROUP BY Month, FECHA_EXTRACCION, ACT_ACCT_cd, PD_VO_PROD_ID,pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE, CST_CHRN_DT
),

CustomerBase_BOM AS(

    SELECT DISTINCT DATE_TRUNC(DATE_ADD(Fecha_Extraccion, INTERVAL 1 MONTH),MONTH) AS Month, Fecha_Extraccion AS B_DATE, act_acct_cd AS AccountBOM, pd_vo_prod_id as B_VO_id, pd_vo_prod_nm as B_VO_nm, pd_tv_prod_id AS B_TV_id, 
    pd_tv_prod_cd as B_TV_nm, pd_bb_prod_id as B_BB_id, pd_bb_prod_nm as B_BB_nm, RGU_VO as B_RGU_VO, RGU_TV as B_RGU_TV, RGU_BB AS B_RGU_BB, fi_outst_age as B_Overdue, C_CUST_AGE as B_Tenure, MaxInst as B_MaxInst,
    RGU_VO + RGU_TV + RGU_BB AS B_NumRGUs,
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
    FROM CR_UsefulFields
    WHERE Fecha_extraccion = Month_Last_Day
)

,CustomerBase_EOM AS(
    
    SELECT DISTINCT DATE_TRUNC(Fecha_Extraccion,MONTH) AS Month, Fecha_Extraccion as E_Date, act_acct_cd as AccountEOM, pd_vo_prod_id as E_VO_id, pd_vo_prod_nm as E_VO_nm, pd_tv_prod_cd AS E_TV_id, 
    pd_tv_prod_cd as E_TV_nm, pd_bb_prod_id as E_BB_id, pd_bb_prod_nm as E_BB_nm, RGU_VO as E_RGU_VO, RGU_TV as E_RGU_TV, RGU_BB AS E_RGU_BB, fi_outst_age as E_Overdue, C_CUST_AGE as E_Tenure, MaxInst as E_MaxInst,
    RGU_VO + RGU_TV + RGU_BB AS E_NumRGUs,
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
    FROM CR_UsefulFields
    WHERE Fecha_extraccion = Month_Last_Day 
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
   B_Date, B_VO_id, B_VO_nm, B_TV_id, B_TV_nm, B_BB_id, B_BB_nm, B_RGU_VO, B_RGU_TV, B_RGU_BB, B_NumRGUs, B_Overdue, B_Tenure, B_MaxInst, B_Bundle_Type, B_BundleName,
   E_Date, E_VO_id, E_VO_nm, E_TV_id, E_TV_nm, E_BB_id, E_BB_nm, E_RGU_VO, E_RGU_TV, E_RGU_BB, E_NumRGUs, E_Overdue, E_Tenure, E_MaxInst, E_Bundle_Type, E_BundleName
  FROM CustomerBase_BOM b FULL OUTER JOIN CustomerBase_EOM e ON b.AccountBOM = e.AccountEOM AND b.Month = e.Month
)

,FixedCustomerBase_MvmtFlag AS(

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
/*
-- Opening/Closing Customer Base AS(
SELECT Fixed_Month, SUM(ActiveBOM) as OpeningBase, Sum(ActiveEOM) as ClosingBase
FROM FixedCustomerBase_MvmtFlag
WHERE (E_OVERDUE<=90 OR E_OVERDUE IS NULL)
GROUP BY Fixed_Month
ORDER BY Fixed_Month)*/

 /*--,Opening/Closing RGU Base
SELECT Fixed_Month, SUM(B_RGU_VO) AS OPENING_VO, SUM(B_RGU_TV) AS OPENING_TV, SUM (B_RGU_BB) AS OPENING_BB, SUM(B_NumRGUs) as Opening_TotalRGUs,
SUM(E_RGU_VO) AS CLOSING_VO, SUM(E_RGU_TV) AS CLOSING_TV, SUM (E_RGU_BB) AS CLOSING_BB, SUM(E_NumRGUs) as Closing_TotalRGUs
FROM FixedCustomerBase_MvmtFlag
WHERE (E_OVERDUE<=90 OR E_OVERDUE IS NULL)
GROUP BY Fixed_Month
ORDER BY Fixed_Month*/

-- Customer Movements KPI
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
/*
SELECT Fixed_Month, MainMovement, SUM(DIF_RGU_VO) AS ChangeVO,SUM(DIF_RGU_TV) AS ChangeTV, SUM(DIF_RGU_BB) AS ChangeBB, SUM (DIF_TOTAL_RGU) AS ChangeRGUs
FROM FixedCustomerBase_MvmtFlag
WHERE Fixed_Month = '2022-02-01' AND (MainMovement = "Upsell" OR MainMovement = "Downsell")
GROUP BY Fixed_Month, MainMovement, GainMovement
ORDER BY Fixed_Month, MainMovement, GainMovement*/

----------------------------------------------Churners---------------------------------------------------------

,CHURNERSCRM AS(
    SELECT DISTINCT ACT_ACCT_CD, MAX(ChurnDate) AS Maxfecha,  DATE_TRUNC(Max(ChurnDate),month) AS MesChurnF
    FROM  CR_UsefulFields
    GROUP BY ACT_ACCT_CD
    HAVING DATE_TRUNC(safe_cast(Maxfecha as date), MONTH) = DATE_TRUNC(MAX(FECHA_EXTRACCION),MONTH)
),
FIRSTCHURN AS(
 SELECT DISTINCT ACT_ACCT_CD, Min(ChurnDate) AS PrimerChurn, Extract(Month from Min(ChurnDate)) AS MesChurnP
    FROM  CR_UsefulFields
    GROUP BY ACT_ACCT_CD
),
REALCHURNERS AS(
 SELECT DISTINCT c.ACT_ACCT_CD, MaxFecha, PrimerChurn, MesChurnF, MesChurnP
 FROM CHURNERSCRM c  INNER JOIN FIRSTCHURN f ON c.ACT_ACCT_CD = f.ACT_ACCT_CD AND f.PrimerChurn <= c.MaxFecha
   GROUP BY ACT_ACCT_CD, MaxFecha, PrimerChurn, MesChurnF, MesChurnP
),

INVOLUNTARYCHURNERS AS(
 SELECT DISTINCT ACT_ACCT_CD, Min(ChurnDate) AS InvolChurnDate,
    FROM  CR_UsefulFields
    WHERE (FI_OUTST_AGE >= 60 AND PD_BB_PROD_ID IS NOT NULL) 
   OR (FI_OUTST_AGE >= 90 AND (PD_TV_PROD_ID IS NOT NULL OR PD_VO_PROD_ID IS NOT NULL))
    GROUP BY ACT_ACCT_CD

)
,CHURNTYPEFINALCHURNERS AS(
 SELECT DISTINCT c.ACT_ACCT_CD, MaxFecha, PrimerChurn, MesChurnF, MesChurnP,
 CASE WHEN i.ACT_ACCT_CD IS NULL THEN C.ACT_ACCT_CD END AS Voluntario,
 CASE WHEN i.ACT_ACCT_CD IS NOT NULL THEN C.ACT_ACCT_CD END AS Involuntario
 FROM REALCHURNERS c  LEFT JOIN INVOLUNTARYCHURNERS i ON c.ACT_ACCT_CD = i.ACT_ACCT_CD AND i.InvolChurnDate <= MaxFecha
GROUP BY ACT_ACCT_CD, MaxFecha, PrimerChurn, MesChurnF, MesChurnP, Voluntario, Involuntario
)
,ChurnersTotales AS (
SELECT ACT_ACCT_CD,
MesChurnF, voluntario, involuntario
FROM CHURNTYPEFINALCHURNERS
GROUP BY MesChurnF, ACT_ACCT_CD, voluntario, involuntario
ORDER BY MesChurnF
)
,ChurnerMovementTable AS(
SELECT *,
CASE 
WHEN voluntario is not null then "Voluntary"
WHEN involuntario is not null THEN "Involuntary" Else null end as churners,
FROM ChurnersTotales
)

,TENURECHURNERS AS(
 SELECT DISTINCT ACT_ACCT_CD, MIN(ChurnDate) as MinChurnDate,
 CASE   WHEN MAX(C_CUST_AGE) <= 180 THEN "Early Tenure"
        WHEN MAX(C_CUST_AGE) > 180 THEN "Long Tenure"
        END AS TENURE
 FROM CR_UsefulFields
 GROUP BY ACT_ACCT_CD
)
,CHURNTYPETENURE AS(
    SELECT DISTINCT c.*, t.Tenure
    FROM CHURNTYPEFINALCHURNERS c INNER JOIN TENURECHURNERS t ON c.ACT_ACCT_CD = t.ACT_ACCT_CD AND c.PrimerChurn = t.MinChurnDate
)
--,UnionChurners AS (
SELECT f.*,voluntario, involuntario, tenure
FROM FixedCustomerBase_MvmtFlag f LEFT JOIN CHURNTYPETENURE 
ON Fixed_account=act_acct_cd AND DATE_TRUNC(safe_cast(Fixed_month as date),month)=DATE_TRUNC(safe_cast(MesChurnF as date),month)
/*)
--------------------------------Voluntary/Involuntary  Early Tenure/LateTenure-------------------------------------
SELECT Fixed_month,Tenure, Count(voluntario) AS VoluntaryChurners, count(involuntario) AS InvoluntaryChurners
FROM UnionChurners
WHERE tenure is not null
GROUP BY Fixed_Month,Tenure
ORDER BY Fixed_Month
------------------------------------------------------------------------------------------------------------------
*/
