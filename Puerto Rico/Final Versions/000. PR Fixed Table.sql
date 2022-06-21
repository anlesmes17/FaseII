With
------------------------------------------------------- Fixed Useful Fields -----------------------------------------------------------------------------------


UsefulFields AS(
Select Distinct sub_acct_no_sbb,home_phone_sbb,email,drop_type,connect_dte_sbb,bridger_addr_hse,play_type,tenure as TenureDays, 
CASE 
WHEN Tenure <0.5 THEN 'Early Tenure'
WHEN Tenure >=0.5 THEN 'Late Tenure'
ELSE NULL END AS TenureType,

CASE
WHEN as_of='2022-01-31' THEN Date('2022-01-31')
WHEN as_of='2022-02-28' THEN Date('2022-02-28')
WHEN as_of='2022-03-31' THEN Date('2022-03-31')
WHEN as_of='2022-04-30' THEN Date('2022-04-30')
WHEN as_of='2022-05-31' THEN Date('2022-05-31')
ELSE NULL END AS FECHA_EXTRACCION,
CASE 
WHEN play_type='1P HSD' OR play_type='1P Video' OR play_type='1P Voice' THEN '1P'
WHEN play_type='2P Video HSD' OR play_type='2P HSD Voice' OR play_type='2P Video HSD' THEN '2P'
WHEN play_type='3P Video HSD Voice' THEN '3P'
ELSE NULL END AS BundleType,
round((hsd_chrg + video_chrg + voice_chrg),0) AS TOT_MRC

FROM "lcpr_fix_ext_csr"
WHERE cust_typ_sbb='RES'
)


,CustomerBase_BOM AS(
SELECT DISTINCT DATE_TRUNC('Month',FECHA_EXTRACCION) AS Month, sub_acct_no_sbb AS AccountBOM,home_phone_sbb AS B_Phone,email AS B_EMAIL,
drop_type AS B_TECH,connect_dte_sbb AS B_StrtDate,bridger_addr_hse AS B_Node,TenureDays AS B_Tenure, TenureType AS B_TenureType,BundleType AS B_BundleType, 
TOT_MRC AS B_MRC,play_type AS B_play_type
FROM UsefulFields
)

,CustomerBase_EOM AS(
SELECT DISTINCT DATE_ADD('Month', -1,DATE_TRUNC('Month',FECHA_EXTRACCION)) AS Month, sub_acct_no_sbb AS AccountEOM,home_phone_sbb AS E_Phone,email AS E_EMAIL,
drop_type AS E_TECH,connect_dte_sbb AS E_StrtDate,bridger_addr_hse AS E_Node,TenureDays AS E_Tenure, TenureType AS E_TenureType,BundleType AS E_BundleType, 
TOT_MRC AS E_MRC, play_type AS E_play_type
FROM UsefulFields
)

--,FixedCustomerBase AS(
SELECT DISTINCT
CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN b.Month
     WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN e.Month
     END AS Fixed_Month,
CASE WHEN (accountBOM IS NOT NULL AND accountEOM IS NOT NULL) OR (accountBOM IS NOT NULL AND accountEOM IS NULL) THEN accountBOM
     WHEN (accountBOM IS NULL AND accountEOM IS NOT NULL) THEN accountEOM
     END AS Fixed_Account,
CASE WHEN accountBOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveBOM,
CASE WHEN accountEOM IS NOT NULL THEN 1 ELSE 0 END AS ActiveEOM,

B_Phone,B_EMAIL,B_TECH,B_StrtDate,B_Node,B_Tenure,B_TenureType,B_BundleType,B_MRC,B_play_type,
E_Phone,E_EMAIL,E_TECH,E_StrtDate,E_Node,E_Tenure,E_TenureType,E_BundleType,E_MRC,E_play_type


FROM CustomerBase_BOM b FULL OUTER JOIN CustomerBase_EOM e ON b.AccountBOM = e.AccountEOM AND b.Month = e.Month
--)
/*
,MainMovementBase AS(
Select f.*, CASE
)
*/
