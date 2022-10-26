WITH 


Fixed_Base AS(
  SELECT DISTINCT * FROM "lla_cco_int_san"."cr_fixed_table"

)

,Mobile_Base AS(
  SELECT DISTINCT * FROM "lla_cco_int_san"."cr_mobile_table"

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


--,EMAIL_EOM AS (
SELECT DISTINCT FECHA_PARQUE,replace(ID_ABONADO,'.','') as ID_ABONADO, act_acct_cd, NOM_EMAIL AS E_EMAIL
FROM "dna_mobile_historic_cr" inner join "db-analytics-dev"."dna_fixed_cr" 
ON 
--cast(dt as varchar)=FECHA_PARQUE AND 
NOM_EMAIL=ACT_CONTACT_MAIL_1 
WHERE DES_SEGMENTO_CLIENTE <>'Empresas - Empresas' AND DES_SEGMENTO_CLIENTE <>'Empresas - Pymes' 
AND NOM_EMAIL <>'NOTIENE@GMAIL.COM' AND NOM_EMAIL<> 'NOREPORTA@CABLETICA.COM' AND NOM_EMAIL<>'NOREPORTACORREO@CABLETICA.COM'
--)

,NEARFMC_MOBILE_EOM AS (
SELECT DISTINCT a.*,b.act_acct_cd AS E_CONTR, E_EMAIL
FROM NEARFMC_MOBILE_BOM a LEFT JOIN EMAIL_EOM b 
ON ID_ABONADO=Mobile_Account AND DATE_ADD('Month',-1,cast(dt as date) )=Mobile_Month
)


--,CONTRATO_ADJ AS (
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
--)
