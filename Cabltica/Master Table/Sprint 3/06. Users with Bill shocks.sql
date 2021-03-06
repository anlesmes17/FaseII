WITH 



Fixed_Base AS(
  SELECT DISTINCT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Fixed_DashboardInput_v2`

)

,Mobile_Base AS(
  SELECT DISTINCT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Mobile_DashboardInput`

)

########################################### Near FMC ######################################################

--------------------------------------------- FEB ------------------------------------------------------------

,EMAIL_FEB AS (
    SELECT DISTINCT FECHA_PARQUE,replace(ID_ABONADO,".","") as ID_ABONADO, act_acct_cd, NOM_EMAIL AS B_EMAIL
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220411_cabletica_fmc_febrero`
    INNER JOIN `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-02-16_FINAL_HISTORIC_CRM_FILE_2021_D`
    ON FECHA_EXTRACCION=FECHA_PARQUE AND NOM_EMAIL=ACT_CONTACT_MAIL_1
     WHERE DES_SEGMENTO_CLIENTE <>"Empresas - Empresas" AND DES_SEGMENTO_CLIENTE <>"Empresas - Pymes" 
     AND NOM_EMAIL <>"NOTIENE@GMAIL.COM" AND NOM_EMAIL<> "NOREPORTA@CABLETICA.COM" AND NOM_EMAIL<>"NOREPORTACORREO@CABLETICA.COM"
)

,NEARFMC_MOBILE_FEB AS (
    SELECT DISTINCT a.*,b.act_acct_cd AS B_CONTR, b.B_EMAIL
    FROM Mobile_Base a LEFT JOIN EMAIL_FEB b 
    ON ID_ABONADO=Mobile_Account AND FECHA_PARQUE=Mobile_Month
)
------------------------------------------------------------------- Mar -------------------------------------------------------
,EMAIL_MAR AS (
    SELECT DISTINCT FECHA_PARQUE,replace(ID_ABONADO,".","") as ID_ABONADO, act_acct_cd, NOM_EMAIL AS E_EMAIL
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220411_cabletica_fmc_marzo`
    INNER JOIN `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220405_CRM_march_sample`
    ON FECHA_EXTRACCION=FECHA_PARQUE AND NOM_EMAIL=ACT_CONTACT_MAIL_1
     WHERE DES_SEGMENTO_CLIENTE <>"Empresas - Empresas" AND DES_SEGMENTO_CLIENTE <>"Empresas - Pymes" 
     AND NOM_EMAIL <>"NOTIENE@GMAIL.COM" AND NOM_EMAIL<> "NOREPORTA@CABLETICA.COM" AND NOM_EMAIL<>"NOREPORTACORREO@CABLETICA.COM"
)

,NEARFMC_MOBILE_MAR AS (
    SELECT DISTINCT a.*,b.act_acct_cd AS E_CONTR, E_EMAIL
    FROM NEARFMC_MOBILE_FEB a LEFT JOIN EMAIL_MAR b 
    ON ID_ABONADO=Mobile_Account AND DATE_SUB(FECHA_PARQUE, INTERVAL 1 Month)=Mobile_Month
    
)
,CONTRATO_ADJ AS (
    SELECT a.*,
    CASE WHEN B_CONTRATO IS NOT NULL THEN safe_cast(B_CONTRATO as string)
    WHEN B_CONTR IS NOT NULL THEN safe_cast(B_CONTR as string)
    ELSE NULL
    END AS B_Mobile_Contrato_Adj,
    CASE WHEN E_CONTRATO IS NOT NULL THEN safe_cast(E_CONTRATO as string)
    WHEN E_CONTR IS NOT NULL THEN safe_cast(E_CONTR as string)
    ELSE NULL
    END AS E_Mobile_Contrato_Adj
    FROM NEARFMC_MOBILE_MAR a
)

,MobilePreliminaryBase AS (
    SELECT a.*,
    CASE WHEN B_Mobile_Contrato_Adj IS NOT NULL THEN B_Mobile_Contrato_Adj
      WHEN E_Mobile_Contrato_Adj IS NOT NULL THEN E_Mobile_Contrato_Adj
      END AS Mobile_Contrato_Adj
     FROM CONTRATO_ADJ a
)
,AccountFix AS (
    SELECT DISTINCT Mobile_Contrato_Adj, Mobile_Month, COUNT(*) as FixedCount
    FROM MobilePreliminaryBase
    GROUP BY Mobile_Contrato_Adj, Mobile_Month
)

,JoinAccountFix AS (
    SELECT m.*, b.FixedCount
    FROM MobilePreliminaryBase m LEFT JOIN AccountFix b ON m.Mobile_Contrato_Adj=b.Mobile_Contrato_Adj AND m.Mobile_Month=b.Mobile_month

)


############################################## Join Fixed Mobile ################################################


,FullCustomerBase AS(
SELECT DISTINCT
CASE WHEN (Fixed_Account IS NOT NULL AND Mobile_Account IS NOT NULL) OR (Fixed_Account IS NOT NULL AND Mobile_Account IS NULL) THEN safe_cast(Fixed_Month as string)
      WHEN (Fixed_Account IS NULL AND Mobile_Account IS NOT NULL) THEN safe_cast(Mobile_Month as string)
  END AS Month,
CASE WHEN (Fixed_Account IS NOT NULL AND Mobile_Account IS NOT NULL) OR (Fixed_Account IS NOT NULL AND Mobile_Account IS NULL) THEN safe_cast(Fixed_Account as string)
      WHEN (Fixed_Account IS NULL AND Mobile_Account IS NOT NULL) THEN safe_cast(Mobile_Account as string)
  END AS Final_Account,
CASE WHEN (ActiveBOM =1 AND Mobile_ActiveBOM=1) or (ActiveBOM=1 AND (Mobile_ActiveBOM=0 or Mobile_ActiveBOM IS NULL)) or ((ActiveBOM=0 OR ActiveBOM IS NULL) AND Mobile_ActiveBOM=1) THEN 1
ELSE 0 END AS Final_BOM_ActiveFlag,
CASE WHEN (ActiveEOM =1 AND Mobile_ActiveEOM=1) or (ActiveEOM=1 AND (Mobile_ActiveEOM=0 or Mobile_ActiveEOM IS NULL)) or ((ActiveEOM=0 OR ActiveEOM IS NULL) AND Mobile_ActiveEOM=1) THEN 1
ELSE 0 END AS Final_EOM_ActiveFlag,
CASE 
WHEN (Fixed_Account is not null and Mobile_Account is not null and ActiveBOM = 1 and Mobile_ActiveBOM = 1 AND B_CONTRATO IS NOT NULL ) THEN "Soft FMC"
WHEN (B_EMAIL IS NOT NULL AND B_CONTRATO IS NULL AND ActiveBOM=1) OR (ActiveBOM = 1 and Mobile_ActiveBOM = 1)  THEN "Near FMC"
WHEN (B_EMAIL IS NOT NULL AND B_CONTRATO IS NOT NULL AND Fixed_Account IS NOT NULL AND ActiveBOM=1)  THEN "Undefined FMC"
WHEN (Fixed_Account IS NOT NULL AND ActiveBOM=1 AND (Mobile_ActiveBOM = 0 OR Mobile_ActiveBOM IS NULL))  THEN "Fixed Only"
WHEN ((Mobile_Account IS NOT NULL AND Mobile_ActiveBOM=1 AND (ActiveBOM = 0 OR ActiveBOM IS NULL)))  THEN "Mobile Only"
  END AS B_FMC_Status,
CASE 
WHEN FixedCount IS NULL THEN 1
WHEN FixedCount IS NOT NULL THEN FixedCount
End as ContractsFix,
CASE 
WHEN (Fixed_Account is not null and Mobile_Account is not null and ActiveEOM = 1 and Mobile_ActiveEOM = 1 AND E_CONTRATO IS NOT NULL ) THEN "Soft FMC"
WHEN (E_EMAIL IS NOT NULL AND E_CONTRATO IS NULL) OR (ActiveEOM = 1 and Mobile_ActiveEOM = 1 ) THEN "Near FMC"
WHEN (E_EMAIL IS NOT NULL AND E_CONTRATO IS NOT NULL)   THEN "Undefined FMC"
WHEN (Fixed_Account IS NOT NULL AND ActiveEOM=1 AND (Mobile_ActiveEOM = 0 OR Mobile_ActiveEOM IS NULL))  THEN "Fixed Only"
WHEN (Mobile_Account IS NOT NULL AND Mobile_ActiveEOM=1 AND (ActiveEOM = 0 OR ActiveEOM IS NULL )) AND MobileChurnFlag IS NULL THEN "Mobile Only"
 END AS E_FMC_Status, f.*,m.* EXCEPT(B_EMAIL, E_EMAIL, B_CONTR, E_CONTR, B_Mobile_Contrato_Adj, E_Mobile_Contrato_Adj, Mobile_Account, MESES_ANTIGUEDAD),
FROM Fixed_Base f FULL OUTER JOIN JoinAccountFix  m 
ON safe_cast(Fixed_Account as string)=Mobile_Contrato_Adj AND Fixed_Month=Mobile_Month
)

,CustomerBase_FMC_Tech_Flags AS(
 
 SELECT t.*,
  round(ifnull(B_BILL_AMT/ContractsFix,0) + ifnull(ROUND(SAFE_CAST(replace(RENTA,".","") AS NUMERIC),0),0),0) AS TOTAL_B_MRC ,  round(ifnull(E_BILL_AMT/ContractsFix,0) + ifnull(ROUND(SAFE_CAST(replace(RENTA,".","") AS NUMERIC),0),0),0) AS TOTAL_E_MRC,
 CASE  
 WHEN (B_FMC_Status = "Fixed Only" OR B_FMC_Status = "Soft FMC" OR B_FMC_Status="Near FMC" )  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL) AND B_MIX = "1P" THEN "Fixed 1P"
 WHEN (B_FMC_Status = "Fixed Only" OR B_FMC_Status = "Soft FMC" OR B_FMC_Status="Near FMC" )  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL) AND B_MIX = "2P" THEN "Fixed 2P"
 WHEN (B_FMC_Status = "Fixed Only" OR B_FMC_Status = "Soft FMC" OR B_FMC_Status="Near FMC" )  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL) AND B_MIX = "3P" THEN "Fixed 3P"
 WHEN B_FMC_Status = "Undefined FMC" AND  MOBILE_ACTIVEBOM=1 THEN "Mobile Only"
 WHEN (B_FMC_Status = "Soft FMC" ) AND (ActiveBOM = 0 OR ActiveBOM is null) then "Mobile Only"
 WHEN B_FMC_Status = "Mobile Only" THEN B_FMC_Status
 WHEN (B_FMC_Status="Near FMC" OR  B_FMC_Status="Soft FMC") THEN B_FMC_Status
 END AS B_FMCType,
 CASE 
 WHEN Final_EOM_ActiveFlag = 0 AND (ActiveEOM = 0 AND FixedChurnType IS NULL) OR (Mobile_ActiveEOM = 0 AND MobileChurnFlag is null) THEN "Customer Gap"
 WHEN E_FMC_Status = "Fixed Only" AND FixedChurnType IS NOT NULL THEN NULL
 WHEN E_FMC_Status = "Mobile Only" AND MobileChurnFlag IS NOT NULL THEN NULL
 WHEN (E_FMC_Status = "Fixed Only"  )  AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR(Mobile_ActiveEOM = 1 AND MobileChurnFlag IS NOT NULL))  AND E_MIX = "1P" THEN "Fixed 1P"
 WHEN (E_FMC_Status = "Fixed Only" )  AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR(Mobile_ActiveEOM = 1 AND MobileChurnFlag IS NOT NULL)) AND E_MIX = "2P" THEN "Fixed 2P"
 WHEN (E_FMC_Status = "Fixed Only" )  AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR(Mobile_ActiveEOM = 1 AND MobileChurnFlag IS NOT NULL)) AND E_MIX = "3P" THEN "Fixed 3P"
 WHEN (E_FMC_Status = "Soft FMC" OR E_FMC_Status = "Near FMC" OR E_FMC_Status = "Undefined FMC"  ) AND (ActiveEOM = 0 OR ActiveEOM is null OR (ActiveEOM = 1 AND FixedChurnType IS NOT NULL)) then "Mobile Only"
 WHEN E_FMC_Status = "Mobile Only" OR((ActiveEOM is null or activeeom=0) and(Mobile_ActiveEOM=1)) THEN "Mobile Only"
 WHEN E_FMC_Status="Soft FMC" AND (FixedChurnType IS NULL AND MobileChurnFlag IS NULL AND Fixed_Account IS NOT NULL  AND ActiveEOM=1 AND Mobile_ActiveEOM=1 ) THEN E_FMC_Status
 WHEN E_FMC_Status="Near FMC" AND (FixedChurnType IS NULL AND MobileChurnFlag IS NULL  AND Fixed_Account IS NOT NULL  AND ActiveEOM=1 AND Mobile_ActiveEOM=1) THEN E_FMC_Status
 WHEN E_FMC_Status="Undefined FMC" AND (FixedChurnType IS NULL AND MobileChurnFlag IS NULL  AND Fixed_Account IS NOT NULL AND  ActiveEOM=1 AND Mobile_ActiveEOM=1) THEN E_FMC_Status
 END AS E_FMCType,
 FROM FullCustomerBase  t
 
)

,CustomerBase_FMCSegments_ChurnFlag AS(
SELECT c.*, 
 CASE WHEN (B_FMC_Status = "Fixed Only") OR ((B_FMC_Status = "Soft FMC" OR B_FMC_Status="Near FMC" OR B_FMC_Status="Undefined FMC") AND ACTIVEBOM = 1 AND Mobile_ActiveBOM = 1) THEN B_TechAdj
 WHEN B_FMC_Status = "Mobile Only" OR ((B_FMC_Status = "Soft FMC" OR B_FMC_Status="Near FMC" OR B_FMC_Status="Undefined FMC") AND (ACTIVEBOM = 0 or ACTIVEBOM IS NULL)) THEN "Wireless"
 END AS B_FinalTechFlag,
 CASE
 WHEN (E_FMC_Status = "Fixed Only" AND FixedChurnType is null) OR ((E_FMC_Status = "Soft FMC" OR E_FMC_Status="Near FMC" OR E_FMC_Status="Undefined FMC" ) AND ACTIVEEOM = 1 AND Mobile_ActiveEOM = 1 AND FixedChurnType is null) THEN E_TechAdj
 WHEN E_FMC_Status = "Mobile Only" OR ((E_FMC_Status = "Soft FMC" OR E_FMC_Status="Near FMC" OR E_FMC_Status="Undefined FMC") AND (ACTIVEEOM = 0 OR ActiveEOM IS NULL)) THEN "Wireless"
 END AS E_FinalTechFlag,
 CASE WHEN (B_TenureType =  "Late Tenure" and TenureCustomer =  "Late Tenure") OR (B_TenureType =  "Late Tenure" and TenureCustomer is null) or (B_TenureType IS NULL and TenureCustomer =  "Late Tenure") THEN "Late Tenure"
 WHEN (B_TenureType =  "Early Tenure" OR TenureCustomer =  "Early Tenure") THEN "Early Tenure"
 END AS B_TenureFinalFlag,
 CASE WHEN (E_TenureType =  "Late Tenure" and TenureCustomer =  "Late Tenure") OR (E_TenureType =  "Late Tenure" and TenureCustomer is null) or (E_TenureType IS NULL and TenureCustomer =  "Late Tenure") THEN "Late Tenure"
 WHEN (E_TenureType =  "Early Tenure" OR TenureCustomer =  "Early Tenure") THEN "Early Tenure"
 END AS E_TenureFinalFlag,
CASE
WHEN (B_FMCType = "Soft FMC" OR B_FMCType = "Near FMC" OR B_FMCType = "Undefined FMC") AND B_MIX = "1P"  THEN "P2"
WHEN (B_FMCType  = "Soft FMC" OR B_FMCType = "Near FMC" OR B_FMCType = "Undefined FMC") AND B_MIX = "2P" THEN "P3"
WHEN (B_FMCType  = "Soft FMC" OR B_FMCType = "Near FMC" OR B_FMCType = "Undefined FMC") AND B_MIX = "3P" THEN "P4"
WHEN (B_FMCType  = "Fixed 1P" OR B_FMCType  = "Fixed 2P" OR B_FMCType  = "Fixed 3P") OR ((B_FMCType  = "Soft FMC" OR B_FMCType="Near FMC" OR B_FMCType="Undefined FMC") AND(Mobile_ActiveBOM= 0 OR Mobile_ActiveBOM IS NULL)) AND ActiveBOM = 1 THEN "P1_Fixed"
WHEN (B_FMCType = "Mobile Only")  OR (B_FMCType  = "Soft FMC" AND(ActiveBOM= 0 OR ActiveBOM IS NULL)) AND Mobile_ActiveBOM = 1 THEN "P1_Mobile"
END AS B_FMC_Segment,
CASE WHEN E_FMCType="Customer Gap" THEN "Customer Gap" 
WHEN (E_FMCType = "Soft FMC" OR E_FMCType="Near FMC" OR E_FMCType="Undefined FMC") AND (ActiveEOM = 1 and Mobile_ActiveEOM=1) AND E_MIX = "1P" AND (FixedChurnType IS NULL and MobileChurnFlag IS NULL) THEN "P2"
WHEN (E_FMCType  = "Soft FMC" OR E_FMCType="Near FMC" OR E_FMCType="Undefined FMC") AND (ActiveEOM = 1 and Mobile_ActiveEOM=1) AND E_MIX = "2P" AND (FixedChurnType IS NULL and MobileChurnFlag IS NULL) THEN "P3"
WHEN (E_FMCType  = "Soft FMC" OR E_FMCType="Near FMC" OR E_FMCType="Undefined FMC") AND (ActiveEOM = 1 and Mobile_ActiveEOM=1) AND E_MIX = "3P" AND (FixedChurnType IS NULL and MobileChurnFlag IS NULL) THEN "P4"

WHEN ((E_FMCType  = "Fixed 1P" OR E_FMCType  = "Fixed 2P" OR E_FMCType  = "Fixed 3P") OR ((E_FMCType  = "Soft FMC" OR E_FMCType="Near FMC" OR E_FMCType="Undefined FMC") AND(Mobile_ActiveEOM= 0 OR Mobile_ActiveEOM IS NULL))) AND (ActiveEOM = 1 AND FixedChurnType IS NULL) THEN "P1_Fixed"
WHEN ((E_FMCType = "Mobile Only")  OR (E_FMCType  = "Soft FMC" AND(ActiveEOM= 0 OR ActiveEOM IS NULL))) AND (Mobile_ActiveEOM = 1 and MobileChurnFlag IS NULL) THEN "P1_Mobile"
END AS E_FMC_Segment,
CASE WHEN (FixedChurnType is not null  AND (ActiveBOM IS NULL OR ACTIVEBOM = 0)) OR (MobileChurnFlag is not null and (Mobile_ActiveBOM = 0 or Mobile_ActiveBOM IS NULL)) THEN "Churn Exception"
WHEN (FixedChurnType is not null and MobileChurnFlag is not null) then "Full Churner"
WHEN (FixedChurnType is not null and MobileChurnFlag is null) then "Fixed Churner"
WHEN (FixedChurnType is null and MobileChurnFlag is NOT null) then "Mobile Churner"
WHEN (FixedChurnType is null and MobileChurnFlag is null and (ActiveEOM = 0 OR ActiveEOM is null) AND (Mobile_ActiveEOM = 0 or Mobile_ActiveEOM IS NULL)) THEN "Customer Gap"
ELSE "Non Churner" END AS FinalChurnFlag,
 round(ifnull(TOTAL_E_MRC,0) - ifnull(TOTAL_B_MRC,0),0) AS MRC_Change
FROM CustomerBase_FMC_Tech_Flags c
)

,RejoinerColumn AS (
  SELECT DISTINCT  f.*
,CASE WHEN Fixed_RejoinerFeb = 1 AND E_FMC_Segment = "P1_Fixed" THEN "Fixed Rejoiner"
WHEN (Fixed_RejoinerFeb = 1) OR ((Fixed_RejoinerFeb = 1) and  (E_FMCType = "Soft FMC" OR E_FMCType = "Near FMC")) THEN "FMC Rejoiner"
END AS Rejoiner_FinalFlag,
FROM CustomerBase_FMCSegments_ChurnFlag f
--WHERE Month = '2022-02-01' 
)



############################################ Waterfall ######################################################



,FullCustomersBase_Flags_Waterfall AS(

SELECT DISTINCT f.* except(E_FMC_Segment),
CASE WHEN (B_FMCTYPE="Fixed 1P" OR B_FMCType="Fixed 2P" OR B_FMCType="Fixed 3P" ) AND E_FMCType="Mobile Only" AND FinalChurnFlag<>"Churn Exception"
AND FinalChurnFlag<>"Customer Gap" AND FinalChurnFlag<>"Fixed Churner" THEN "Customer Gap"
ELSE E_FMC_Segment END AS E_FMC_Segment
,CASE 
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_NumRGUs < E_NumRGUs) AND ((Mobile_ActiveBOM=Mobile_ActiveEOM) OR (Mobile_ActiveBOM is null and Mobile_ActiveEOM is null) ) THEN "Upsell"
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_NumRGUs > E_NumRGUs) AND ((Mobile_ActiveBOM=Mobile_ActiveEOM) OR (Mobile_ActiveBOM is null and Mobile_ActiveEOM is null) ) THEN "Downsell"
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_NumRGUs = E_NumRGUs) AND (TOTAL_B_MRC = TOTAL_E_MRC) THEN "Maintain"
WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1) AND ((MainMovement = "New Customer" AND MobileMovementFlag = "3.Gross Add/ rejoiner") OR (MainMovement = "New Customer" AND MobileMovementFlag IS NULL) OR (MainMovement IS NULL AND MobileMovementFlag = "3.Gross Add/ rejoiner")) THEN "Fixed Gross Add or Mobile Gross Add/ Rejoiner"
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_NumRGUs = E_NumRGUs) AND (TOTAL_B_MRC> TOTAL_E_MRC ) THEN "Downspin"
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_NumRGUs = E_NumRGUs) AND (TOTAL_B_MRC < TOTAL_E_MRC) THEN "Upspin"
WHEN Rejoiner_FinalFlag IS NOT NULL THEN Rejoiner_FinalFlag
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (ACTIVEBOM IS NULL AND ACTIVEEOM IS NULL) AND (Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=1) THEN "Maintain"
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (FinalChurnFlag="Fixed Churner") AND ((Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=1) OR (Mobile_ActiveBOM IS NULL AND Mobile_ActiveEOM IS NULL) ) THEN "Fixed Churner"
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag =0) AND (FinalChurnFlag="Fixed Churner") AND ((Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=1) OR (Mobile_ActiveBOM IS NULL AND Mobile_ActiveEOM IS NULL) ) THEN "Fixed Churner"
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag =1) AND (ActiveBOM=0 AND ActiveEOM=1) AND (Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=0) THEN "Fixed Gross Add or Mobile Gross Add/ Rejoiner"
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (FinalChurnFlag="Fixed Churner") AND (Mobile_ActiveBOM=0 AND Mobile_ActiveEOM=1) THEN "Fixed to Mobile Customer Gap"
WHEN FinalChurnFlag="Full Churner" Then "Full Churner"
WHEN FinalChurnFlag="Mobile Churner" Then "Mobile Churner"
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag =1) AND (ActiveBOM=1 AND ActiveEOM=0) AND FinalChurnFlag="Non Churner" THEN "Churn Gap"
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (Mobile_ActiveBOM=0 AND Mobile_ActiveEOM=1) AND (ActiveBOM=1 AND ActiveEOM=1) THEN "Mobile Gross Adds"
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (ActiveBOM=1 AND ActiveEOM=1) AND (Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=0) THEN "Mobile Churner"
WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1) AND E_FMC_Segment="P1_Fixed" AND Rejoiner_FinalFlag is null then "Fixed Gross Add or Mobile Gross Add/ Rejoiner"
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (ActiveBOM=0 AND ActiveEOM=1) AND (Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=1) THEN "FMC Packing"
WHEN B_FMC_Status="Undefined FMC" OR E_FMC_Status="Undefined FMC" THEN "Gap Undefined FMC"
WHEN FinalChurnFlag="Customer Gap" THEN "Customer Gap"
WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1)  AND (ActiveBOM=0 AND ActiveEOM=1) AND (Mobile_ActiveBOM=0 AND Mobile_ActiveEOM=1) THEN "FMC Gross Add"
WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1)  AND (ActiveBOM=0 AND ActiveEOM=1) AND FixedChurnType IS NOT NULL THEN "Customer Gap" 
END AS Waterfall_Flag
FROM RejoinerColumn f
)

,AbsMRC AS (
SELECT *, abs(mrc_change) AS Abs_MRC_Change FROM FullCustomersBase_Flags_Waterfall
WHERE (B_FMC_Segment IN('P1_Fixed','P2','P3','P4') OR E_FMC_Segment IN('P1_Fixed','P2','P3','P4'))
)
,BillShocks AS (
SELECT *,
CASE
WHEN Abs_MRC_Change>(TOTAL_B_MRC*(.05)) THEN "Bill Schock" ELSE NULL END AS BillShocksKPI
FROM AbsMRC
WHERE B_PLAN=E_PLAN
)

,BaseNoChanges AS (
SELECT Month, COUNT(DISTINCT Fixed_Account) AS AccountsNoChanges FROM BillShocks
GROUP BY 1
)
,CountBillShocks AS (
  SELECT Month, COUNT(DISTINCT Fixed_Account) AS BillChanges 
  FROM BillShocks
  WHERE BillShocksKPI IS NOT NULL
  GROUP BY 1
)
SELECT b.Month, AccountsNoChanges, BillChanges
FROM BaseNoChanges b LEFT JOIN CountBillShocks c ON b.Month=c.Month
ORDER BY Month
