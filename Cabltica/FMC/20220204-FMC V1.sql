WITH 
CRM AS (
SELECT * 
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-02-16_FINAL_HISTORIC_CRM_FILE_2021_D`
WHERE safe_cast(FECHA_EXTRACCION AS String)="2022-02-27"
)

,FMC AS(
SELECT replace(safe_cast(ID_ABONADO AS string), ".", "") AS identification, NOM_EMAIL
FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220404_cabletica_fmc_febrero`
WHERE DES_PRODUCTO<>"NO INFORMADO"
)

--,USERS AS (
SELECT DISTINCT safe_cast(act_acct_cd AS String) AS act_acct_cd,CST_cust_name, 
Identification--, NOM_EMAIL, ACT_CONTACT_MAIL_1
FROM CRM c INNER JOIN FMC f ON f.NOM_EMAIL= c.ACT_CONTACT_MAIL_1
WHERE NOM_EMAIL <>"NOTIENE@GMAIL.COM" AND NOM_EMAIL<> "NOREPORTA@CABLETICA.COM"
--ORDER BY act_acct_cd
/*)

,AccountColumn AS (
SELECT *,
CASE 
WHEN act_acct_cd IS NULL AND Identification IS NOT NULL THEN Identification
WHEN act_acct_cd IS NOT NULL AND Identification IS NOT NULL THEN Identification
WHEN act_acct_cd IS NOT NULL AND Identification IS NULL THEN act_acct_cd
ELSE NULL END AS Account
FROM USERS
)
SELECT DISTINCT Account FROM AccountColumn */
