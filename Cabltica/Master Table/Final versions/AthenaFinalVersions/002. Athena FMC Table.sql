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
ON ID_ABONADO=Mobile_Account AND DATE_ADD('Month',-1,cast('dt' as date) )=cast(Mobile_Month as date)
)

/*
,CONTRATO_ADJ AS (
SELECT a.*,
CASE WHEN B_FMCAccount IS NOT NULL THEN cast(B_FMCAccount as varchar)
WHEN B_CONTR IS NOT NULL THEN cast(B_CONTR as varchar)
ELSE NULL
END AS B_Mobile_Contrato_Adj,
CASE WHEN E_FMCAccount IS NOT NULL THEN cast(E_FMCAccount as varchar)
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

--,FullCustomerBase as(
SELECT DISTINCT
CASE WHEN (Fixed_Account IS NOT NULL AND Mobile_Account IS NOT NULL) OR (Fixed_Account IS NOT NULL AND Mobile_Account IS NULL) THEN Fixed_Month
      WHEN (Fixed_Account IS NULL AND Mobile_Account IS NOT NULL) THEN Mobile_Month
  END AS Month,
CASE 
WHEN (Fixed_Account IS NOT NULL AND Mobile_Account IS NULL) THEN Fixed_Account
WHEN (Fixed_Account IS NULL AND Mobile_Account IS NOT NULL) THEN Mobile_Account
WHEN (Fixed_Account IS NOT NULL AND Mobile_Account IS NOT NULL) THEN Concat('Fixed_Account','Mobile_Account')
END AS Final_Account,
CASE 
WHEN (ActiveBOM =1 AND Mobile_ActiveBOM=1) or (ActiveBOM=1 AND (Mobile_ActiveBOM=0 or Mobile_ActiveBOM IS NULL)) or ((ActiveBOM=0 OR ActiveBOM IS NULL) AND Mobile_ActiveBOM=1) THEN 1 ELSE 0 END AS Final_BOM_ActiveFlag,
CASE 
WHEN (ActiveEOM =1 AND Mobile_ActiveEOM=1) or (ActiveEOM=1 AND (Mobile_ActiveEOM=0 or Mobile_ActiveEOM IS NULL)) or ((ActiveEOM=0 OR ActiveEOM IS NULL) AND Mobile_ActiveEOM=1) THEN 1
ELSE 0 END AS Final_EOM_ActiveFlag,
CASE
WHEN (Fixed_Account is not null and Mobile_Account is not null and ActiveBOM = 1 and Mobile_ActiveBOM = 1 AND B_FMCAccount IS NOT NULL ) THEN "Soft FMC"
WHEN (B_EMAIL IS NOT NULL AND B_FMCAccount IS NULL AND ActiveBOM=1) OR (ActiveBOM = 1 and Mobile_ActiveBOM = 1)  THEN "Near FMC"
WHEN (Fixed_Account IS NOT NULL AND ActiveBOM=1 AND (Mobile_ActiveBOM = 0 OR Mobile_ActiveBOM IS NULL))  THEN "Fixed Only"
WHEN ((Mobile_Account IS NOT NULL AND Mobile_ActiveBOM=1 AND (ActiveBOM = 0 OR ActiveBOM IS NULL)))  THEN "Mobile Only"
END AS B_FMC_Status,
CASE 
WHEN (Fixed_Account is not null and Mobile_Account is not null and ActiveEOM = 1 and Mobile_ActiveEOM = 1 AND E_FMCAccount IS NOT NULL ) THEN "Soft FMC"
WHEN (E_EMAIL IS NOT NULL AND E_FMCAccount IS NULL AND ActiveEOM=1) OR (ActiveEOM = 1 and Mobile_ActiveEOM = 1) THEN "Near FMC"
WHEN (Fixed_Account IS NOT NULL AND ActiveEOM=1 AND (Mobile_ActiveEOM = 0 OR Mobile_ActiveEOM IS NULL))  THEN "Fixed Only"
WHEN (Mobile_Account IS NOT NULL AND Mobile_ActiveEOM=1 AND (ActiveEOM = 0 OR ActiveEOM IS NULL )) AND MobileChurnFlag IS NULL THEN "Mobile Only"
END AS E_FMC_Status
FROM Fixed_Base f FULL OUTER JOIN NEARFMC_MOBILE_EOM  m 
ON Fixed_Account=FMC_Account AND Fixed_Month=Mobile_Month
--)
