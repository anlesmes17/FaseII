WITH 
CRM AS (
SELECT * 
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-02-16_FINAL_HISTORIC_CRM_FILE_2021_D`
WHERE safe_cast(FECHA_EXTRACCION AS String)="2022-02-27"
)

,CONVERGENCE AS(
SELECT replace(safe_cast(ID_ABONADO AS string), ".", "") AS identification, NOM_EMAIL
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220404_cabletica_fmc_febrero`
WHERE DES_PRODUCTO<>"NO INFORMADO"
)

,USERS AS (
SELECT DISTINCT safe_cast(act_acct_cd AS String) AS act_acct_cd,CST_cust_name, 
Identification--, NOM_EMAIL, ACT_CONTACT_MAIL_1
FROM CRM c INNER JOIN CONVERGENCE f ON f.NOM_EMAIL= c.ACT_CONTACT_MAIL_1
WHERE NOM_EMAIL <>"NOTIENE@GMAIL.COM" AND NOM_EMAIL<> "NOREPORTA@CABLETICA.COM"
)
,FlagCRM AS (
    SELECT safe_cast(c.act_acct_cd AS String) as act_acct_cd,c.ACT_CONTACT_MAIL_1,
    CASE 
    WHEN u.act_acct_cd IS NOT NULL THEN "FMC"
    ELSE NULL END AS FMCCRM
    FROM CRM c LEFT JOIN USERS u ON safe_cast(c.act_acct_cd as string)=u.act_acct_cd
)
,FlagConvergence AS (
    SELECT f.Identification, NOM_EMAIL,
    CASE
    WHEN u.Identification IS NOT NULL THEN "FMC"
    ELSE NULL END AS FMCConvergence
    FROM CONVERGENCE f LEFT JOIN USERS u ON f.Identification=u.Identification
)
,FMCUSERS AS (
    SELECT c.act_acct_cd AS contrato, Identification,
    CASE
    WHEN c.act_acct_cd IS NULL AND Identification IS NOT NULL THEN Identification
    WHEN Identification IS NULL AND c.act_acct_cd IS NOT NULL THEN act_acct_cd
    WHEN c.act_acct_cd IS NOT NULL AND Identification IS NOT NULL AND (FMCCRM="FMC" OR FMCConvergence="FMC") THEN Identification
    WHEN c.act_acct_cd IS NOT NULL AND Identification IS NOT NULL AND (FMCCRM <>"FMC" OR FMCConvergence <>"FMC") THEN act_acct_cd
    ELSE NULL END AS UserFMC
    FROM FlagCRM c FULL OUTER JOIN FlagConvergence f ON f.NOM_EMAIL= c.ACT_CONTACT_MAIL_1
)
SELECT DISTINCT UserFMC
FROM FMCUSERS
