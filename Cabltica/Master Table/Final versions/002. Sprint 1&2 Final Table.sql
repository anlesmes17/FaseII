--create or replace table `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-07-14_Cabletica_FMC_DashboardInput` as 
WITH 


Fixed_Base AS(
  SELECT DISTINCT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Fixed_DashboardInput_v2`

)

,Mobile_Base AS(
  SELECT DISTINCT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Mobile_DashboardInput_v2`

)

########################################### Near FMC ######################################################

--------------------------------------------- FEB ------------------------------------------------------------

,EMAIL_BOM AS (
    SELECT DISTINCT FECHA_PARQUE,replace(ID_ABONADO,".","") as ID_ABONADO, act_acct_cd, NOM_EMAIL AS B_EMAIL
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220524_cabletica_mobile_DNA` 
    INNER JOIN `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-06-08_CR_HISTORIC_CRM_ENE_2021_MAY_2022`
    ON safe_cast(FECHA_EXTRACCION as string)=FECHA_PARQUE AND NOM_EMAIL=ACT_CONTACT_MAIL_1
     WHERE DES_SEGMENTO_CLIENTE <>"Empresas - Empresas" AND DES_SEGMENTO_CLIENTE <>"Empresas - Pymes" 
     AND NOM_EMAIL <>"NOTIENE@GMAIL.COM" AND NOM_EMAIL<> "NOREPORTA@CABLETICA.COM" AND NOM_EMAIL<>"NOREPORTACORREO@CABLETICA.COM"
)

,NEARFMC_MOBILE_BOM AS (
    SELECT DISTINCT a.*,b.act_acct_cd AS B_CONTR, b.B_EMAIL
    FROM Mobile_Base a LEFT JOIN EMAIL_BOM b 
    ON ID_ABONADO=Mobile_Account AND safe_cast(FECHA_PARQUE as string)=safe_cast(Mobile_Month as string)
)
------------------------------------------------------------------- Mar -------------------------------------------------------
,EMAIL_EOM AS (
    SELECT DISTINCT FECHA_PARQUE,replace(ID_ABONADO,".","") as ID_ABONADO, act_acct_cd, NOM_EMAIL AS E_EMAIL
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220524_cabletica_mobile_DNA` 
    INNER JOIN `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-06-08_CR_HISTORIC_CRM_ENE_2021_MAY_2022`
    ON safe_cast(FECHA_EXTRACCION as string)=FECHA_PARQUE AND NOM_EMAIL=ACT_CONTACT_MAIL_1
     WHERE DES_SEGMENTO_CLIENTE <>"Empresas - Empresas" AND DES_SEGMENTO_CLIENTE <>"Empresas - Pymes" 
     AND NOM_EMAIL <>"NOTIENE@GMAIL.COM" AND NOM_EMAIL<> "NOREPORTA@CABLETICA.COM" AND NOM_EMAIL<>"NOREPORTACORREO@CABLETICA.COM"
)

,NEARFMC_MOBILE_EOM AS (
    SELECT DISTINCT a.*,b.act_acct_cd AS E_CONTR, E_EMAIL
    FROM NEARFMC_MOBILE_BOM a LEFT JOIN EMAIL_EOM b 
    ON ID_ABONADO=Mobile_Account AND DATE_SUB(safe_cast(FECHA_PARQUE as date), INTERVAL 1 Month)=Mobile_Month
    
)
,CONTRATO_ADJ AS (
    SELECT a.*,
    CASE WHEN B_FMCAccount IS NOT NULL THEN safe_cast(B_FMCAccount as string)
    WHEN B_CONTR IS NOT NULL THEN safe_cast(B_CONTR as string)
    ELSE NULL
    END AS B_Mobile_Contrato_Adj,
    CASE WHEN E_FMCAccount IS NOT NULL THEN safe_cast(E_FMCAccount as string)
    WHEN E_CONTR IS NOT NULL THEN safe_cast(E_CONTR as string)
    ELSE NULL
    END AS E_Mobile_Contrato_Adj
    FROM NEARFMC_MOBILE_EOM a
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
WHEN (Fixed_Account is not null and Mobile_Account is not null and ActiveBOM = 1 and Mobile_ActiveBOM = 1 AND B_FMCAccount IS NOT NULL ) THEN "Soft FMC"
WHEN (B_EMAIL IS NOT NULL AND B_FMCAccount IS NULL AND ActiveBOM=1) OR (ActiveBOM = 1 and Mobile_ActiveBOM = 1)  THEN "Near FMC"
WHEN (Fixed_Account IS NOT NULL AND ActiveBOM=1 AND (Mobile_ActiveBOM = 0 OR Mobile_ActiveBOM IS NULL))  THEN "Fixed Only"
WHEN ((Mobile_Account IS NOT NULL AND Mobile_ActiveBOM=1 AND (ActiveBOM = 0 OR ActiveBOM IS NULL)))  THEN "Mobile Only"
  END AS B_FMC_Status,
CASE 
WHEN FixedCount IS NULL THEN 1
WHEN FixedCount IS NOT NULL THEN FixedCount
End as ContractsFix,
CASE 
WHEN (Fixed_Account is not null and Mobile_Account is not null and ActiveEOM = 1 and Mobile_ActiveEOM = 1 AND E_FMCAccount IS NOT NULL ) THEN "Soft FMC"
WHEN (E_EMAIL IS NOT NULL AND E_FMCAccount IS NULL) OR (ActiveEOM = 1 and Mobile_ActiveEOM = 1 ) THEN "Near FMC"
WHEN (Fixed_Account IS NOT NULL AND ActiveEOM=1 AND (Mobile_ActiveEOM = 0 OR Mobile_ActiveEOM IS NULL))  THEN "Fixed Only"
WHEN (Mobile_Account IS NOT NULL AND Mobile_ActiveEOM=1 AND (ActiveEOM = 0 OR ActiveEOM IS NULL )) AND MobileChurnFlag IS NULL THEN "Mobile Only"
 END AS E_FMC_Status, f.*,m.* EXCEPT(B_EMAIL, E_EMAIL, B_CONTR, E_CONTR, B_Mobile_Contrato_Adj, E_Mobile_Contrato_Adj),
FROM Fixed_Base f FULL OUTER JOIN JoinAccountFix  m 
ON safe_cast(Fixed_Account as string)=Mobile_Contrato_Adj AND Fixed_Month=Mobile_Month
)

,CustomerBase_FMC_Tech_Flags AS(
 
 SELECT t.*,
  round(round(ifnull(B_BILL_AMT/ContractsFix,0)) + ifnull(Mobile_MRC_BOM,0),0) AS TOTAL_B_MRC ,  round(round(ifnull(E_BILL_AMT/ContractsFix,0)) + ifnull(Mobile_MRC_EOM,0),0) AS TOTAL_E_MRC,
 CASE  
 WHEN (B_FMC_Status = "Fixed Only" OR B_FMC_Status = "Soft FMC" OR B_FMC_Status="Near FMC" )  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL) AND B_MIX = "1P" THEN "Fixed 1P"
 WHEN (B_FMC_Status = "Fixed Only" OR B_FMC_Status = "Soft FMC" OR B_FMC_Status="Near FMC" )  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL) AND B_MIX = "2P" THEN "Fixed 2P"
 WHEN (B_FMC_Status = "Fixed Only" OR B_FMC_Status = "Soft FMC" OR B_FMC_Status="Near FMC" )  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL) AND B_MIX = "3P" THEN "Fixed 3P"
 WHEN (B_FMC_Status = "Soft FMC" ) AND (ActiveBOM = 0 OR ActiveBOM is null) then "Mobile Only"
 WHEN B_FMC_Status = "Mobile Only" THEN B_FMC_Status
 WHEN (B_FMC_Status="Near FMC" OR  B_FMC_Status="Soft FMC") THEN B_FMC_Status
 END AS B_FMCType,
 CASE 
 WHEN Final_EOM_ActiveFlag = 0 AND ((ActiveEOM = 0 AND FixedChurnTypeFlag IS NULL) OR (Mobile_ActiveEOM = 0 AND MobileChurnFlag is null)) THEN "Customer Gap"
 WHEN E_FMC_Status = "Fixed Only" AND FixedChurnTypeFlag IS NOT NULL THEN NULL
 WHEN E_FMC_Status = "Mobile Only" AND MobileChurnFlag IS NOT NULL THEN NULL
 WHEN (E_FMC_Status = "Fixed Only"  )  AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR(Mobile_ActiveEOM = 1 AND MobileChurnFlag IS NOT NULL))  AND E_MIX = "1P" THEN "Fixed 1P"
 WHEN (E_FMC_Status = "Fixed Only" )  AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR(Mobile_ActiveEOM = 1 AND MobileChurnFlag IS NOT NULL)) AND E_MIX = "2P" THEN "Fixed 2P"
 WHEN (E_FMC_Status = "Fixed Only" )  AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR(Mobile_ActiveEOM = 1 AND MobileChurnFlag IS NOT NULL)) AND E_MIX = "3P" THEN "Fixed 3P"
 WHEN (E_FMC_Status = "Soft FMC" OR E_FMC_Status = "Near FMC" OR E_FMC_Status = "Undefined FMC"  ) AND (ActiveEOM = 0 OR ActiveEOM is null OR (ActiveEOM = 1 AND FixedChurnTypeFlag IS NOT NULL)) then "Mobile Only"
 WHEN E_FMC_Status = "Mobile Only" OR((ActiveEOM is null or activeeom=0) and(Mobile_ActiveEOM=1)) THEN "Mobile Only"
 WHEN E_FMC_Status="Soft FMC" AND (FixedChurnTypeFlag IS NULL AND MobileChurnFlag IS NULL AND Fixed_Account IS NOT NULL  AND ActiveEOM=1 AND Mobile_ActiveEOM=1 ) THEN E_FMC_Status
 WHEN E_FMC_Status="Near FMC" AND (FixedChurnTypeFlag IS NULL AND MobileChurnFlag IS NULL  AND Fixed_Account IS NOT NULL  AND ActiveEOM=1 AND Mobile_ActiveEOM=1) THEN E_FMC_Status
 WHEN E_FMC_Status="Undefined FMC" AND (FixedChurnTypeFlag IS NULL AND MobileChurnFlag IS NULL  AND Fixed_Account IS NOT NULL AND  ActiveEOM=1 AND Mobile_ActiveEOM=1) THEN E_FMC_Status
 END AS E_FMCType
,case when Mobile_ActiveBOM=1 then 1 else 0 end as B_MobileRGUs
,case when Mobile_ActiveEOM=1 then 1 else 0 end as E_MobileRGUs
 FROM FullCustomerBase  t
 
)

,CustomerBase_FMCSegments_ChurnFlag AS(
SELECT c.*, 
 CASE WHEN (B_FMC_Status = "Fixed Only") OR ((B_FMC_Status = "Soft FMC" OR B_FMC_Status="Near FMC" OR B_FMC_Status="Undefined FMC") AND ACTIVEBOM = 1 AND Mobile_ActiveBOM = 1) THEN B_TechAdj
 WHEN B_FMC_Status = "Mobile Only" OR ((B_FMC_Status = "Soft FMC" OR B_FMC_Status="Near FMC" OR B_FMC_Status="Undefined FMC") AND (ACTIVEBOM = 0 or ACTIVEBOM IS NULL)) THEN "Wireless"
 END AS B_FinalTechFlag,
 CASE
 WHEN (E_FMC_Status = "Fixed Only" AND FixedChurnTypeFlag is null) OR ((E_FMC_Status = "Soft FMC" OR E_FMC_Status="Near FMC" OR E_FMC_Status="Undefined FMC" ) AND ACTIVEEOM = 1 AND Mobile_ActiveEOM = 1 AND FixedChurnTypeFlag is null) THEN E_TechAdj
 WHEN E_FMC_Status = "Mobile Only" OR ((E_FMC_Status = "Soft FMC" OR E_FMC_Status="Near FMC" OR E_FMC_Status="Undefined FMC") AND (ACTIVEEOM = 0 OR ActiveEOM IS NULL)) THEN "Wireless"
 END AS E_FinalTechFlag,
 CASE WHEN (B_FixedTenureSegment =  "Late Tenure" and B_MobileTenureSegment =  "Late Tenure") OR (B_FixedTenureSegment =  "Late Tenure" and B_MobileTenureSegment is null) or (B_FixedTenureSegment IS NULL and B_MobileTenureSegment =  "Late Tenure") THEN "Late Tenure"
 WHEN (B_FixedTenureSegment =  "Early Tenure" OR B_MobileTenureSegment =  "Early Tenure") THEN "Early Tenure"
 END AS B_TenureFinalFlag,
 CASE WHEN (E_FixedTenureSegment =  "Late Tenure" and E_MobileTenureSegment =  "Late Tenure") OR (E_FixedTenureSegment =  "Late Tenure" and E_MobileTenureSegment is null) or (E_FixedTenureSegment IS NULL and E_MobileTenureSegment =  "Late Tenure") THEN "Late Tenure"
 WHEN (E_FixedTenureSegment =  "Early Tenure" OR E_MobileTenureSegment =  "Early Tenure") THEN "Early Tenure"
 END AS E_TenureFinalFlag,
CASE
WHEN (B_FMCType = "Soft FMC" OR B_FMCType = "Near FMC" OR B_FMCType = "Undefined FMC") AND B_MIX = "1P"  THEN "P2"
WHEN (B_FMCType  = "Soft FMC" OR B_FMCType = "Near FMC" OR B_FMCType = "Undefined FMC") AND B_MIX = "2P" THEN "P3"
WHEN (B_FMCType  = "Soft FMC" OR B_FMCType = "Near FMC" OR B_FMCType = "Undefined FMC") AND B_MIX = "3P" THEN "P4"
WHEN (B_FMCType  = "Fixed 1P" OR B_FMCType  = "Fixed 2P" OR B_FMCType  = "Fixed 3P") OR ((B_FMCType  = "Soft FMC" OR B_FMCType="Near FMC" OR B_FMCType="Undefined FMC") AND(Mobile_ActiveBOM= 0 OR Mobile_ActiveBOM IS NULL)) AND ActiveBOM = 1 THEN "P1_Fixed"
WHEN (B_FMCType = "Mobile Only")  OR (B_FMCType  = "Soft FMC" AND(ActiveBOM= 0 OR ActiveBOM IS NULL)) AND Mobile_ActiveBOM = 1 THEN "P1_Mobile"
END AS B_FMC_Segment,
CASE WHEN E_FMCType="Customer Gap" THEN "Customer Gap" 
WHEN (E_FMCType = "Soft FMC" OR E_FMCType="Near FMC" OR E_FMCType="Undefined FMC") AND (ActiveEOM = 1 and Mobile_ActiveEOM=1) AND E_MIX = "1P" AND (FixedChurnTypeFlag IS NULL and MobileChurnFlag IS NULL) THEN "P2"
WHEN (E_FMCType  = "Soft FMC" OR E_FMCType="Near FMC" OR E_FMCType="Undefined FMC") AND (ActiveEOM = 1 and Mobile_ActiveEOM=1) AND E_MIX = "2P" AND (FixedChurnTypeFlag IS NULL and MobileChurnFlag IS NULL) THEN "P3"
WHEN (E_FMCType  = "Soft FMC" OR E_FMCType="Near FMC" OR E_FMCType="Undefined FMC") AND (ActiveEOM = 1 and Mobile_ActiveEOM=1) AND E_MIX = "3P" AND (FixedChurnTypeFlag IS NULL and MobileChurnFlag IS NULL) THEN "P4"

WHEN ((E_FMCType  = "Fixed 1P" OR E_FMCType  = "Fixed 2P" OR E_FMCType  = "Fixed 3P") OR ((E_FMCType  = "Soft FMC" OR E_FMCType="Near FMC" OR E_FMCType="Undefined FMC") AND(Mobile_ActiveEOM= 0 OR Mobile_ActiveEOM IS NULL))) AND (ActiveEOM = 1 AND FixedChurnTypeFlag IS NULL) THEN "P1_Fixed"
WHEN ((E_FMCType = "Mobile Only")  OR (E_FMCType  = "Soft FMC" AND(ActiveEOM= 0 OR ActiveEOM IS NULL))) AND (Mobile_ActiveEOM = 1 and MobileChurnFlag IS NULL) THEN "P1_Mobile"
END AS E_FMC_Segment,
CASE WHEN (FixedChurnTypeFlag is not null  AND (ActiveBOM IS NULL OR ACTIVEBOM = 0)) OR (MobileChurnFlag is not null and (Mobile_ActiveBOM = 0 or Mobile_ActiveBOM IS NULL)) THEN "Churn Exception"
WHEN (FixedChurnTypeFlag is not null and MobileChurnFlag is not null) then "Churner"
WHEN (FixedChurnTypeFlag is not null and MobileChurnFlag is null) then "Fixed Churner"
WHEN FixedChurnTypeFlag is null and activebom=1 and mobile_activebom=1 AND (activeeom=0 or activeeom is null) and (Mobile_ActiveEOM=0 or mobile_activeeom Is null) THEN "Full Churner"
WHEN (FixedChurnTypeFlag is null and MobileChurnFlag is NOT null) then "Mobile Churner"
WHEN ActiveBom=1 AND ActiveEOM=0 AND Mobile_Month IS NULL THEN "Fixed churner - Customer Gap"
ELSE "Non Churner" END AS FinalChurnFlag
,((coalesce(B_NumRGUs,0) + coalesce(B_MobileRGUs,0))/ContractsFix) as B_TotalRGUs
,((coalesce(E_NumRGUs,0) + coalesce(E_MobileRGUs,0))/ContractsFix) AS E_TotalRGUs
,round(ifnull(TOTAL_E_MRC,0) - ifnull(TOTAL_B_MRC,0),0) AS MRC_Change
FROM CustomerBase_FMC_Tech_Flags c
)

,RejoinerColumn AS (
  SELECT DISTINCT  f.*
,CASE WHEN Fixed_Rejoiner = 1 AND E_FMC_Segment = "P1_Fixed" THEN "Fixed Rejoiner"
WHEN (Fixed_Rejoiner = 1) OR ((Fixed_Rejoiner = 1) and  (E_FMCType = "Soft FMC" OR E_FMCType = "Near FMC")) THEN "FMC Rejoiner"
WHEN Mobile_Rejoinermonth IS NOT NULL AND E_FMC_Segment = "P1_Mobile" THEN "Mobile Rejoiner"
END AS Rejoiner_FinalFlag,
FROM CustomerBase_FMCSegments_ChurnFlag f
)



############################################ Waterfall ######################################################



,FullCustomersBase_Flags_Waterfall AS(
SELECT DISTINCT f.* except(E_FMC_Segment),
CASE WHEN (FinalChurnFlag = "Churner") OR ((FinalChurnFlag = 'Fixed Churner' OR FinalChurnFlag = "Fixed churner - Customer Gap") and B_FMC_Segment = "P1_Fixed" and (E_FMC_Segment is null or E_FMC_Segment="Customer Gap")) OR (FinalChurnFlag = "Mobile Churner" and B_FMC_Segment = "P1_Mobile" and E_FMC_Segment is null) then 'Total Churner'
WHEN FinalChurnFlag = "Non Churner" then null
ELSE 'Partial Churner' end as Partial_Total_ChurnFlag,
--Arreglar cuando se tenga churn split
CASE
WHEN ((FinalChurnFlag="Full Churner" OR FinalChurnFlag="Fixed Churner" OR FinalChurnFlag="Fixed churner - Customer Gap" AND (Mobile_ActiveEOM=0 OR Mobile_ActiveEOM IS NULL)) AND FixedChurnTypeFlag="Voluntario" AND MobileChurnFlag IS NULL) OR (MobileChurnFlag="BAJA VOLUNTARIA" AND(ActiveBOM=0 OR ActiveEOM IS NULL)) Then "Voluntary"
WHEN ((FinalChurnFlag="Full Churner" OR FinalChurnFlag="Fixed Churner" OR FinalChurnFlag="Fixed churner - Customer Gap" AND (Mobile_ActiveEOM=0 OR Mobile_ActiveEOM IS NULL)) AND FixedChurnTypeFlag="Involuntario" AND MobileChurnFlag IS NULL) OR ((MobileChurnFlag="BAJA INVOLUNTARIA" OR MobileChurnFlag="ALTA/MIGRACION") AND(ActiveBOM=0 OR ActiveEOM IS NULL)) Then "Involuntary"


--WHEN (ActiveEOM=0 OR ActiveEOM IS NULL) AND MobileChurnFlag IS NOT NULL THEN "TBD"
End as churntypefinalflag,




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
WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1)  AND (ActiveBOM=0 AND ActiveEOM=1) AND FixedChurnTypeFlag IS NOT NULL THEN "Customer Gap" 
END AS Waterfall_Flag
FROM RejoinerColumn f
)
,Last_Flags as(
select *
,Case when waterfall_flag='Downsell' and MainMovement='Downsell' then 'Voluntary'
      when waterfall_flag='Downsell' and FinalChurnFlag <> 'Non Churner' then ChurnTypeFinalFlag
      when waterfall_flag='Downsell' and mainmovement='Loss' then 'Undefined'
else null end as Downsell_Split
,case when waterfall_flag='Downspin' then 'Voluntary' else null end as Downspin_Split
from FullCustomersBase_Flags_Waterfall
)

select distinct *
from Last_Flags
