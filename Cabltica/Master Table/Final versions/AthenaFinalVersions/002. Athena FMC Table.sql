WITH 


Fixed_Base AS(
  SELECT DISTINCT * FROM "lla_cco_int_san"."cr_fixed_table"

)

,Mobile_Base AS(
  SELECT DISTINCT * FROM "lla_cco_int_san"."cr_mobile_table" --limit 10

)

---------------------------------------------- Near FMC ---------------------------------

,Near_BOM as(
SELECT DISTINCT FECHA_PARQUE,replace(ID_ABONADO,'.','') as ID_ABONADO, act_acct_cd, NOM_EMAIL AS B_EMAIL
FROM "dna_mobile_historic_cr" inner join "db-analytics-dev"."dna_fixed_cr" 
ON 
--cast(dt as varchar)=FECHA_PARQUE AND 
NOM_EMAIL=ACT_CONTACT_MAIL_1
WHERE DES_SEGMENTO_CLIENTE <>'Empresas - Empresas' AND DES_SEGMENTO_CLIENTE <>'Empresas - Pymes' 
AND NOM_EMAIL <>'NOTIENE@GMAIL.COM' AND NOM_EMAIL<> 'NOREPORTA@CABLETICA.COM' AND NOM_EMAIL<>'NOREPORTACORREO@CABLETICA.COM'
AND NOM_EMAIL<>'NOREPORTA.@CABLETICA.COM' AND NOM_EMAIL<>'NOTIENE@CABLETICA.COM' AND NOM_EMAIL<>'NA@GMAIL.COM'
AND NOM_EMAIL<>'NOTIENE@NOTIENE.COM' AND NOM_EMAIL<>'NOREPORTA@LIBERTY.COM' 
AND NOM_EMAIL<>'NO@GMAIL.COM'
)

,NEARFMC_MOBILE_BOM AS (
SELECT DISTINCT a.*,b.act_acct_cd AS B_CONTR, b.B_EMAIL
FROM Mobile_Base a LEFT JOIN Near_BOM b 
ON ID_ABONADO=Mobile_Account AND cast(FECHA_PARQUE as varchar)=cast(Mobile_Month as varchar)
)


,EMAIL_EOM AS (
SELECT DISTINCT FECHA_PARQUE,replace(ID_ABONADO,'.','') as ID_ABONADO, act_acct_cd, NOM_EMAIL AS E_EMAIL
FROM "dna_mobile_historic_cr" inner join "db-analytics-dev"."dna_fixed_cr" 
ON 
--cast(dt as varchar)=FECHA_PARQUE AND 
NOM_EMAIL=ACT_CONTACT_MAIL_1 
WHERE DES_SEGMENTO_CLIENTE <>'Empresas - Empresas' AND DES_SEGMENTO_CLIENTE <>'Empresas - Pymes' 
AND NOM_EMAIL <>'NOTIENE@GMAIL.COM' AND NOM_EMAIL<> 'NOREPORTA@CABLETICA.COM' AND NOM_EMAIL<>'NOREPORTACORREO@CABLETICA.COM'
)

,NEARFMC_MOBILE_EOM AS (
SELECT DISTINCT a.*,b.act_acct_cd AS E_CONTR, E_EMAIL
FROM NEARFMC_MOBILE_BOM a LEFT JOIN EMAIL_EOM b 
ON ID_ABONADO=Mobile_Account AND DATE_ADD('Month',-1,cast(Fecha_Parque as date) )=cast(Mobile_Month as date)
)

/*
,CONTRATO_ADJ AS (
SELECT a.*,
CASE WHEN B_FixedContract IS NOT NULL THEN cast(B_FixedContract as varchar)
WHEN B_CONTR IS NOT NULL THEN cast(B_CONTR as varchar)
ELSE NULL
END AS B_Mobile_Contrato_Adj,
CASE WHEN E_FixedContract IS NOT NULL THEN cast(E_FixedContract as varchar)
WHEN E_CONTR IS NOT NULL THEN cast(E_CONTR as varchar)
ELSE NULL
END AS E_Mobile_Contrato_Adj
FROM NEARFMC_MOBILE_EOM a
)*/

/*
,JoinAccountFix AS (
    SELECT m.*, b.FixedCount
    FROM MobilePreliminaryBase m LEFT JOIN AccountFix b ON m.Mobile_Contrato_Adj=b.Mobile_Contrato_Adj AND m.Mobile_Month=b.Mobile_month
)
*/

,FullCustomerBase as(
SELECT DISTINCT
CASE WHEN (Fixed_Account IS NOT NULL AND Mobile_Account IS NOT NULL) OR (Fixed_Account IS NOT NULL AND Mobile_Account IS NULL) THEN Fixed_Month
      WHEN (Fixed_Account IS NULL AND Mobile_Account IS NOT NULL) THEN Mobile_Month
  END AS Month,
CASE 
WHEN (Fixed_Account IS NOT NULL AND Mobile_Account IS NULL) THEN Fixed_Account
WHEN (Fixed_Account IS NULL AND Mobile_Account IS NOT NULL) THEN Mobile_Account
WHEN (Fixed_Account IS NOT NULL AND Mobile_Account IS NOT NULL) THEN Concat(Fixed_Account,Mobile_Account)
END AS Final_Account,
CASE 
WHEN (ActiveBOM =1 AND Mobile_ActiveBOM=1) or (ActiveBOM=1 AND (Mobile_ActiveBOM=0 or Mobile_ActiveBOM IS NULL)) or ((ActiveBOM=0 OR ActiveBOM IS NULL) AND Mobile_ActiveBOM=1) THEN 1 ELSE 0 END AS Final_BOM_ActiveFlag,
CASE 
WHEN (ActiveEOM =1 AND Mobile_ActiveEOM=1) or (ActiveEOM=1 AND (Mobile_ActiveEOM=0 or Mobile_ActiveEOM IS NULL)) or ((ActiveEOM=0 OR ActiveEOM IS NULL) AND Mobile_ActiveEOM=1) THEN 1
ELSE 0 END AS Final_EOM_ActiveFlag,
CASE
WHEN (Fixed_Account is not null and Mobile_Account is not null and ActiveBOM = 1 and Mobile_ActiveBOM = 1 AND B_FixedContract IS NOT NULL ) THEN 'Soft FMC'
WHEN (B_EMAIL IS NOT NULL AND B_FixedContract IS NULL AND ActiveBOM=1) OR (ActiveBOM = 1 and Mobile_ActiveBOM = 1)  THEN 'Near FMC'
WHEN (Fixed_Account IS NOT NULL AND ActiveBOM=1 AND (Mobile_ActiveBOM = 0 OR Mobile_ActiveBOM IS NULL))  THEN 'Fixed Only'
WHEN ((Mobile_Account IS NOT NULL AND Mobile_ActiveBOM=1 AND (ActiveBOM = 0 OR ActiveBOM IS NULL)))  THEN 'Mobile Only'
END AS B_FMC_Status,
CASE 
WHEN (Fixed_Account is not null and Mobile_Account is not null and ActiveEOM = 1 and Mobile_ActiveEOM = 1 AND E_FixedContract IS NOT NULL ) THEN 'Soft FMC'
WHEN (E_EMAIL IS NOT NULL AND E_FixedContract IS NULL AND ActiveEOM=1) OR (ActiveEOM = 1 and Mobile_ActiveEOM = 1) THEN 'Near FMC'
WHEN (Fixed_Account IS NOT NULL AND ActiveEOM=1 AND (Mobile_ActiveEOM = 0 OR Mobile_ActiveEOM IS NULL))  THEN 'Fixed Only'
WHEN (Mobile_Account IS NOT NULL AND Mobile_ActiveEOM=1 AND (ActiveEOM = 0 OR ActiveEOM IS NULL )) AND MobileChurnFlag IS NULL THEN 'Mobile Only'
END AS E_FMC_Status,f.*,m.*
FROM Fixed_Base f FULL OUTER JOIN NEARFMC_MOBILE_EOM  m 
ON Fixed_Account=cast(FMC_Account as varchar) AND Fixed_Month=Mobile_Month
)
/*
,RepeatedFix as (
select distinct Month,Final_Account,count(*) From FullCustomerBase
group by 1,2
order by 3 desc
)
*/

,CustomerBase_FMC_Tech_Flags AS(
 SELECT t.*,
coalesce(cast(B_BILL_AMT as integer),0) + coalesce(cast(B_Mobile_MRC as integer),0) AS TOTAL_B_MRC ,  (coalesce(cast(E_BILL_AMT as integer),0) + coalesce(cast(E_Mobile_MRC as integer),0)) AS TOTAL_E_MRC,

CASE  
WHEN (B_FMC_Status = 'Fixed Only' OR B_FMC_Status = 'Soft FMC' OR B_FMC_Status='Near FMC' )  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL) AND B_MIX = '1P' THEN 'Fixed 1P'
WHEN (B_FMC_Status = 'Fixed Only' OR B_FMC_Status = 'Soft FMC' OR B_FMC_Status='Near FMC' )  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL) AND B_MIX = '2P' THEN 'Fixed 2P'
WHEN (B_FMC_Status = 'Fixed Only' OR B_FMC_Status = 'Soft FMC' OR B_FMC_Status='Near FMC' )  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL) AND B_MIX = '3P' THEN 'Fixed 3P'
WHEN (B_FMC_Status = 'Soft FMC' OR B_FMC_Status='Near FMC') AND (ActiveBOM = 0 OR ActiveBOM is null) then 'Mobile Only'
WHEN B_FMC_Status = 'Mobile Only' THEN B_FMC_Status
WHEN (B_FMC_Status='Near FMC' OR  B_FMC_Status='Soft FMC') THEN B_FMC_Status
END AS B_FMCType,

CASE 
--WHEN Final_EOM_ActiveFlag = 0 AND ((ActiveEOM = 0 AND FixedChurnTypeFlag IS NULL) OR (Mobile_ActiveEOM = 0 AND MobileChurnFlag is null)) THEN "Customer Gap"
WHEN E_FMC_Status = 'Fixed Only' AND FixedChurnTypeFlag IS NOT NULL THEN NULL
WHEN E_FMC_Status = 'Mobile Only' AND MobileChurnFlag IS NOT NULL THEN NULL
WHEN (E_FMC_Status = 'Fixed Only')  AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR(Mobile_ActiveEOM = 1 AND MobileChurnFlag IS NOT NULL))  AND E_MIX = '1P' THEN 'Fixed 1P'
WHEN (E_FMC_Status = 'Fixed Only' )  AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR(Mobile_ActiveEOM = 1 AND MobileChurnFlag IS NOT NULL)) AND E_MIX = '2P' THEN 'Fixed 2P'
WHEN (E_FMC_Status = 'Fixed Only' )  AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR(Mobile_ActiveEOM = 1 AND MobileChurnFlag IS NOT NULL)) AND E_MIX = '3P' THEN 'Fixed 3P'
WHEN (E_FMC_Status = 'Soft FMC' OR E_FMC_Status = 'Near FMC') AND (ActiveEOM = 0 OR ActiveEOM is null OR (ActiveEOM = 1 AND FixedChurnTypeFlag IS NOT NULL)) OR (E_FMC_Status = 'Mobile Only' OR((ActiveEOM is null or activeeom=0) and(Mobile_ActiveEOM=1))) THEN 'Mobile Only'
WHEN (E_FMC_Status='Soft FMC' OR E_FMC_Status='Near FMC') AND (FixedChurnTypeFlag IS NULL AND MobileChurnFlag IS NULL AND ActiveEOM=1 AND Mobile_ActiveEOM=1 ) THEN E_FMC_Status
END AS E_FMCType
,case when Mobile_ActiveBOM=1 then 1 else 0 end as B_MobileRGUs
,case when Mobile_ActiveEOM=1 then 1 else 0 end as E_MobileRGUs
 FROM FullCustomerBase t
)

,CustomerBase_FMCSegments_ChurnFlag AS(
SELECT c.*
