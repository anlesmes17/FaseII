--CREATE OR REPLACE TABLE

--`gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Fixed_DashboardInput_v2` AS

###################################### Fixed Useful Fields #################################################################

WITH 
UsefulFields AS(

SELECT DISTINCT DATE_TRUNC (safe_cast(FECHA_EXTRACCION as date), MONTH) AS Month,FECHA_EXTRACCION, act_acct_cd, pd_vo_prod_id, pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE, Min(ACT_ACCT_INST_DT) as MinInst, CST_CHRN_DT AS ChurnDate, DATE_DIFF(safe_cast(FECHA_EXTRACCION as date), safe_cast(OLDEST_UNPAID_BILL_DT as date),DAY) AS MORA, ACT_CONTACT_MAIL_1,
round(VO_FI_TOT_MRC_AMT,0) AS mrcVO, round(BB_FI_TOT_MRC_AMT,0) AS mrcBB, round(TV_FI_TOT_MRC_AMT,0) AS mrcTV,round((VO_FI_TOT_MRC_AMT + BB_FI_TOT_MRC_AMT + TV_FI_TOT_MRC_AMT),0) as avgmrc, round(TOT_BILL_AMT,0) AS Bill, ACT_ACCT_SIGN_DT,
CASE WHEN pd_vo_prod_id IS NOT NULL THEN 1 ELSE 0 END AS RGU_VO,
CASE WHEN pd_tv_prod_cd IS NOT NULL THEN 1 ELSE 0 END AS RGU_TV,
CASE WHEN pd_bb_prod_id IS NOT NULL THEN 1 ELSE 0 END AS RGU_BB,
CASE 
WHEN PD_VO_PROD_ID IS NOT NULL AND PD_BB_PROD_NM IS NOT NULL AND PD_TV_PROD_CD IS NOT NULL THEN "3P"
WHEN PD_VO_PROD_ID IS NULL AND PD_BB_PROD_NM IS NOT NULL AND PD_TV_PROD_CD IS NOT NULL THEN "2P"
WHEN PD_VO_PROD_ID IS NOT NULL AND PD_BB_PROD_NM IS NULL AND PD_TV_PROD_CD IS NOT NULL THEN"2P"
WHEN PD_VO_PROD_ID IS NOT NULL AND PD_BB_PROD_NM IS NOT NULL AND PD_TV_PROD_CD IS NULL THEN"2P"
ELSE "1P" END AS MIX
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-06-08_CR_HISTORIC_CRM_ENE_2021_MAY_2022`
GROUP BY Month, FECHA_EXTRACCION, ACT_ACCT_cd, PD_VO_PROD_ID,pd_vo_prod_nm, PD_TV_PROD_ID,
PD_TV_PROD_CD, pd_bb_prod_id, pd_bb_prod_nm, FI_OUTST_AGE, C_CUST_AGE,CST_CHRN_DT, OLDEST_UNPAID_BILL_DT, VO_FI_TOT_MRC_AMT, BB_FI_TOT_MRC_AMT, TV_FI_TOT_MRC_AMT, VO_FI_TOT_MRC_AMT, BB_FI_TOT_MRC_AMT, TV_FI_TOT_MRC_AMT, ACT_CONTACT_MAIL_1,
mrcVO,mrcBB,mrcTV, Bill,ACT_ACCT_SIGN_DT
)

,CustomerBase_BOM AS(

    SELECT DISTINCT DATE_TRUNC(SAFE_CAST(Fecha_Extraccion AS DATE),MONTH) AS Month, Fecha_Extraccion AS B_DATE, c.act_acct_cd AS AccountBOM, pd_vo_prod_id as B_VO_id, pd_vo_prod_nm as B_VO_nm, pd_tv_prod_id AS B_TV_id, 
    pd_tv_prod_cd as B_TV_nm, pd_bb_prod_id as B_BB_id, pd_bb_prod_nm as B_BB_nm, RGU_VO as B_RGU_VO, RGU_TV as B_RGU_TV, RGU_BB AS B_RGU_BB, fi_outst_age as B_Overdue, C_CUST_AGE as B_Tenure, MinInst as B_MinInst, MIX AS B_MIX,
    RGU_VO + RGU_TV + RGU_BB AS B_NumRGUs, Tipo_Tecnologia AS B_Tech_Type, MORA AS B_MORA, mrcVO as B_VO_MRC, mrcBB as B_BB_MRC, mrcTV as B_TV_MRC, avgmrc as B_AVG_MRC, BILL AS B_BILL_AMT,ACT_ACCT_SIGN_DT AS B_ACT_ACCT_SIGN_DT,
    CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN "1P"
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) OR (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN "2P"
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN "3P" END AS B_Bundle_Type,
    CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) THEN "VO"
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) THEN "TV"
    WHEN (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN "BB"
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) THEN "TV+VO"
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) THEN "BB+TV"
    WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN "BB+VO"
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN "BB+TV+VO" END AS B_BundleName,
    CASE WHEN RGU_BB= 1 THEN act_acct_cd ELSE NULL END As BB_RGU_BOM,
    CASE WHEN RGU_TV= 1 THEN act_acct_cd ELSE NULL END As TV_RGU_BOM,
    CASE WHEN RGU_VO= 1 THEN act_acct_cd ELSE NULL END As VO_RGU_BOM,
    CASE WHEN (RGU_BB = 1 AND RGU_TV = 0 AND RGU_VO = 0) OR  (RGU_BB = 0 AND RGU_TV = 1 AND RGU_VO = 0) OR (RGU_BB = 0 AND RGU_TV = 0 AND RGU_VO = 1)  THEN '1P'
    WHEN (RGU_BB = 1 AND RGU_TV = 1 AND RGU_VO = 0) OR (RGU_BB = 1 AND RGU_TV = 0 AND RGU_VO = 1) OR (RGU_BB = 0 AND RGU_TV = 1 AND RGU_VO = 1) THEN '2P'
    WHEN (RGU_BB = 1 AND RGU_TV = 1 AND RGU_VO = 1) THEN '3P' END AS B_MixCode_Adj,
    FROM UsefulFields c LEFT JOIN `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-13_CR_CATALOGUE_TV_INTERNET_2021_T` ON PD_BB_PROD_nm=ActivoInternet
    WHERE FECHA_EXTRACCION=DATE_TRUNC(FECHA_EXTRACCION,MONTH)
)

,FinalCustomerBase_BOM AS(
    SELECT *,
    CASE 
    WHEN B_Tech_Type IS NOT NULL THEN B_Tech_Type
    WHEN B_Tech_Type IS NULL AND safe_cast(B_RGU_TV AS string)="NEXTGEN TV" THEN "FTTH"
    ELSE "HFC" END AS B_TechAdj,
    CASE
    WHEN B_Tenure <=6 THEN "Early Tenure"
    WHEN B_Tenure >6 THEN "Late Tenure"
    ELSE NULL END AS B_TenureType
    FROM CustomerBase_BOM 
)

,CustomerBase_EOM AS(
    
    SELECT DISTINCT DATE_TRUNC(DATE_SUB(Fecha_Extraccion, INTERVAL 1 MONTH), MONTH) AS Month, Fecha_Extraccion as E_Date, c.act_acct_cd as AccountEOM, pd_vo_prod_id as E_VO_id, pd_vo_prod_nm as E_VO_nm, pd_tv_prod_cd AS E_TV_id, 
    pd_tv_prod_cd as E_TV_nm, pd_bb_prod_id as E_BB_id, pd_bb_prod_nm as E_BB_nm, RGU_VO as E_RGU_VO, RGU_TV as E_RGU_TV, RGU_BB AS E_RGU_BB, fi_outst_age as E_Overdue, C_CUST_AGE as E_Tenure, MinInst as E_MinInst, MIX AS E_MIX,
    RGU_VO + RGU_TV + RGU_BB AS E_NumRGUs,Tipo_Tecnologia AS E_Tech_Type, MORA AS E_MORA, mrcVO AS E_VO_MRC, mrcBB as E_BB_MRC, mrcTV as E_TV_MRC, avgmrc as E_AVG_MRC, BILL AS E_BILL_AMT,ACT_ACCT_SIGN_DT AS E_ACT_ACCT_SIGN_DT,
    CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN "1P"
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) OR (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) OR (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN "2P"
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN "3P" END AS E_Bundle_Type,
    CASE WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 0) THEN "VO"
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 0) THEN "TV"
    WHEN (RGU_VO = 0 AND RGU_TV = 0 AND RGU_BB = 1) THEN "BB"
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 0) THEN "TV+VO"
    WHEN (RGU_VO = 0 AND RGU_TV = 1 AND RGU_BB = 1) THEN "BB+TV"
    WHEN (RGU_VO = 1 AND RGU_TV = 0 AND RGU_BB = 1) THEN "BB+VO"
    WHEN (RGU_VO = 1 AND RGU_TV = 1 AND RGU_BB = 1) THEN "BB+TV+VO" END AS E_BundleName,

     CASE WHEN RGU_BB= 1 THEN act_acct_cd ELSE NULL END As BB_RGU_EOM,
    CASE WHEN RGU_TV= 1 THEN act_acct_cd ELSE NULL END As TV_RGU_EOM,
    CASE WHEN RGU_VO= 1 THEN act_acct_cd ELSE NULL END As VO_RGU_EOM,
    CASE WHEN (RGU_BB = 1 AND RGU_TV = 0 AND RGU_VO = 0) OR  (RGU_BB = 0 AND RGU_TV = 1 AND RGU_VO = 0) OR (RGU_BB = 0 AND RGU_TV = 0 AND RGU_VO = 1)  THEN '1P'
    WHEN (RGU_BB = 1 AND RGU_TV = 1 AND RGU_VO = 0) OR (RGU_BB = 1 AND RGU_TV = 0 AND RGU_VO = 1) OR (RGU_BB = 0 AND RGU_TV = 1 AND RGU_VO = 1) THEN '2P'
    WHEN (RGU_BB = 1 AND RGU_TV = 1 AND RGU_VO = 1) THEN '3P' END AS E_MixCode_Adj,

    FROM UsefulFields c LEFT JOIN `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-13_CR_CATALOGUE_TV_INTERNET_2021_T` ON PD_BB_PROD_nm=ActivoInternet
    WHERE FECHA_EXTRACCION=DATE_TRUNC(FECHA_EXTRACCION,MONTH)
)

,FinalCustomerBase_EOM AS(
    SELECT *,
    CASE 
    WHEN E_Tech_Type IS NOT NULL THEN E_Tech_Type
    WHEN E_Tech_Type IS NULL AND safe_cast(E_RGU_TV AS string)="NEXTGEN TV" THEN "FTTH"
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
   B_Date, B_VO_id, B_VO_nm, B_TV_id, B_TV_nm, B_BB_id, B_BB_nm, B_RGU_VO, B_RGU_TV, B_RGU_BB, B_NumRGUs, B_Overdue, B_Tenure, B_MinInst, B_Bundle_Type, B_BundleName,B_MIX, B_TechAdj,B_TenureType, B_MORA, B_VO_MRC, B_BB_MRC, B_TV_MRC, B_AVG_MRC, B_BILL_AMT,B_ACT_ACCT_SIGN_DT,BB_RGU_BOM,TV_RGU_BOM,VO_RGU_BOM,B_MixCode_Adj,
   E_Date, E_VO_id, E_VO_nm, E_TV_id, E_TV_nm, E_BB_id, E_BB_nm, E_RGU_VO, E_RGU_TV, E_RGU_BB, E_NumRGUs, E_Overdue, E_Tenure, E_MinInst, E_Bundle_Type, E_BundleName,E_MIX, E_TechAdj,E_TenureType, E_MORA, E_VO_MRC, E_BB_MRC, E_TV_MRC, E_AVG_MRC, E_BILL_AMT,E_ACT_ACCT_SIGN_DT,BB_RGU_EOM,TV_RGU_EOM,VO_RGU_EOM,E_MixCode_Adj,
  FROM FinalCustomerBase_BOM b FULL OUTER JOIN FinalCustomerBase_EOM e ON b.AccountBOM = e.AccountEOM AND b.Month = e.Month

)

,ServiceOrders AS (
    SELECT * FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220623_CR_ORDENES_SERVICIO_2021-01_A_2022-05_D`
)



###################################################Main Movements################################################################

,MAINMOVEMENTBASE AS(
 SELECT f.*, CASE
 WHEN (E_NumRGUs - B_NumRGUs)=0 THEN "Same RGUs"
 WHEN (E_NumRGUs - B_NumRGUs)>0 THEN "Upsell"
 WHEN (E_NumRGUs - B_NumRGUs)<0 then "Downsell"
 WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC (E_ACT_ACCT_SIGN_DT, MONTH) <> '2022-02-01') THEN "Come Back to Life"
 WHEN (B_NumRGUs IS NULL AND E_NumRGUs > 0 AND DATE_TRUNC (E_ACT_ACCT_SIGN_DT, MONTH) = '2022-02-01') THEN "New Customer"
 WHEN ActiveBOM = 1 AND ActiveEOM = 0 THEN "Loss"
 END AS MainMovement,

 CASE WHEN ActiveBOM = 0 AND ActiveEOM = 1 AND DATE_TRUNC(E_MinInst,Month) = "2022-02-01" THEN "Feb Gross-Ads"
 WHEN ActiveBOM = 0 AND ActiveEOM = 1 AND DATE_TRUNC(E_MinInst,Month) <> "2022-02-01" THEN "ComeBackToLife/Rejoiners Gross-Ads"
 ELSE NULL END AS GainMovement,
 IFNULL(E_RGU_BB - B_RGU_BB,0) as DIF_RGU_BB , IFNULL(E_RGU_TV - B_RGU_TV,0) as DIF_RGU_TV , IFNULL(E_RGU_VO - B_RGU_VO,0) as DIF_RGU_VO ,
 (E_NumRGUs - B_NumRGUs) as DIF_TOTAL_RGU
 FROM FixedCustomerBase f
)
,SPINMOVEMENTBASE AS (
    SELECT b.*,
    CASE 
    WHEN MainMovement="Same RGUs" AND (E_BILL_AMT - B_BILL_AMT) > 0 THEN "1. Up-spin" 
    WHEN MainMovement="Same RGUs" AND (E_BILL_AMT - B_BILL_AMT) < 0 THEN "2. Down-spin" 
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
 ),
PRIMERCHURNSO AS
(SELECT DISTINCT RIGHT(CONCAT('0000000000',NOMBRE_CONTRATO) ,10) AS CONTRATOSO, Min(FECHA_APERTURA) as PrimerChurnSO,
FROM ServiceOrders
 WHERE
  TIPO_ORDEN = "DESINSTALACION" 
  AND (ESTADO <> "CANCELADA" OR ESTADO <> "ANULADA")
 AND FECHA_APERTURA IS NOT NULL
 GROUP BY CONTRATOSO
 ),
CHURNERSINVOLUNTARIOS AS
(SELECT DISTINCT RIGHT(CONCAT('0000000000',NOMBRE_CONTRATO) ,10) AS CONTRATOSO, FECHA_APERTURA,
 FROM ServiceOrders 
 WHERE
  TIPO_ORDEN = "DESINSTALACION" 
  AND (ESTADO <> "CANCELADA" OR ESTADO <> "ANULADA")
  AND SUBMOTIVO = "MOROSIDAD"
 AND FECHA_APERTURA IS NOT NULL
 ),
CHURNTYPEFLAGSO AS(
    SELECT DISTINCT c. CONTRATOSO, c.PrimerChurnSO,
    CASE WHEN t.CONTRATOSO IS NULL THEN c.contratoso END AS VOLUNTARIO,
    CASE WHEN t.CONTRATOSO IS NOT NULL THEN c.CONTRATOSO END AS INVOLUNTARIO
    FROM PRIMERCHURNSO c LEFT JOIN CHURNERSINVOLUNTARIOS t ON c.CONTRATOSO = t.CONTRATOSO AND t.FECHA_APERTURA = c.PrimerChurnSO
),
CHURNERSCRM AS(
  SELECT DISTINCT RIGHT(CONCAT('0000000000',ACT_ACCT_CD) ,10) AS CHURNER, MAX(DATE(CST_CHRN_DT)) AS Maxfecha,DATE_TRUNC(Max(DATE(CST_CHRN_DT)), MONTH) AS MesChurnF
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-06-08_CR_HISTORIC_CRM_ENE_2021_MAY_2022`
    GROUP BY ACT_ACCT_CD
    HAVING DATE_TRUNC(Maxfecha, MONTH) = DATE_TRUNC(MAX(FECHA_EXTRACCION), MONTH)
),
CRUCECHURNERS AS(
SELECT DISTINCT s.CONTRATOSO, CHURNER, MaxFecha, MesChurnF, Voluntario, Involuntario, Max(FECHA_APERTURA) as Fecha_Apertura
FROM CHURNERSCRM c INNER JOIN CHURNERSSO s ON CONTRATOSO = CHURNER
AND c.MaxFecha >= s.FECHA_APERTURA AND date_diff(c.MaxFecha, s.FECHA_APERTURA, MONTH) <= 3
INNER JOIN CHURNTYPEFLAGSO f ON s.CONTRATOSO = f.CONTRATOSO
GROUP BY contratoso, CHURNER, MesChurnF, MaxFecha, Voluntario, Involuntario
)
,ChurnersFinales AS(
SELECT DISTINCT MesChurnF, MaxFecha as FechaChurn, Fecha_Apertura as FechaMaxSO,
 CHURNER AS CONTRATOCRM, CONTRATOSO AS CONTRATOSERVORDER, 
CASE WHEN VOLUNTARIO IS NOT NULL THEN "Voluntario"
WHEN INVOLUNTARIO IS NOT NULL THEN "Involuntario" END AS FixedChurnType
FROM CRUCECHURNERS
ORDER BY MesChurnF
)

,CustomerBaseWithChurners AS(
 SELECT DISTINCT c.*, RIGHT(CONCAT('0000000000',Fixed_Account) ,10) AS act_acct_cd
 FROM SPINMOVEMENTBASE c 
)

,CRUCECHURNERSCRM AS(
 SELECT DISTINCT C.* except(B_TV_id, E_TV_id,B_BB_id, E_BB_id), FixedChurnType, 
 FROM CustomerBaseWithChurners  c LEFT JOIN ChurnersFinales s ON safe_cast(s.CONTRATOSERVORDER as string)= act_acct_cd AND date_trunc(FechaMaxSO, month) = Fixed_Month
)
########################################## Rejoiners #####################################################

,InactiveUsersMonth AS (
SELECT DISTINCT Fixed_Month AS ExitMonth, Fixed_Account,DATE_ADD(Fixed_Month, INTERVAL 1 MONTH) AS RejoinerMonth
FROM FixedCustomerBase 
WHERE ActiveBOM=1 AND ActiveEOM=0
)

,RejoinersPopulation AS(
SELECT f.*,RejoinerMonth
,CASE WHEN i.Fixed_Account IS NOT NULL THEN 1 ELSE 0 END AS RejoinerPopFlag
## Variabilizar
,CASE WHEN RejoinerMonth>='2022-02-01' AND RejoinerMonth<=DATE_ADD('2022-02-01',INTERVAL 1 MONTH) THEN 1 ELSE 0 END AS Fixed_PR
FROM FixedCustomerBase f LEFT JOIN InactiveUsersMonth i ON f.Fixed_Account=i.Fixed_Account AND Fixed_Month=ExitMonth
)

,FixedRejoinerFebPopulation AS(
SELECT DISTINCT Fixed_Month,RejoinerPopFlag,Fixed_PR,Fixed_Account,'2022-02-01' AS Month
FROM RejoinersPopulation
WHERE RejoinerPopFlag=1
AND Fixed_PR=1
AND Fixed_Month<>'2022-02-01'
GROUP BY 1,2,3,4
)

,FullFixedBase_Rejoiners AS(
SELECT DISTINCT f.*,Fixed_PR
,CASE WHEN Fixed_PR=1 AND MainMovement="Come Back to Life"
THEN 1 ELSE 0 END AS Fixed_Rejoiner
FROM CRUCECHURNERSCRM f LEFT JOIN FixedRejoinerFebPopulation r ON f.Fixed_Account=r.Fixed_Account AND f.Fixed_Month=SAFE_CAST(r.Month AS DATE)
)

--,FinalTable as(
SELECT DISTINCT *, CONCAT(ifnull(B_VO_nm,""),ifnull(B_TV_nm,""),ifnull(B_BB_nm,"")) AS B_PLAN,CONCAT(ifnull(E_VO_nm,""),ifnull(E_TV_nm,""),ifnull(E_BB_nm,"")) AS E_PLAN
FROM FullFixedBase_Rejoiners
order by fixed_month
--)
