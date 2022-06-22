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
WHEN play_type LIKE '%1P%' THEN '1P'
WHEN play_type LIKE '%2P%' THEN '2P'
WHEN play_type LIKE '%3P%' THEN '3P'
ELSE NULL END AS BundleType,
round((hsd_chrg + video_chrg + voice_chrg),0) AS TOT_MRC

FROM "lcpr_fix_ext_csr"
WHERE cust_typ_sbb='RES'
)


,CustomerBase_BOM AS(
SELECT DISTINCT DATE_ADD('Month', 1,DATE_TRUNC('Month',FECHA_EXTRACCION)) AS Month, sub_acct_no_sbb AS AccountBOM,home_phone_sbb AS B_Phone,email AS B_EMAIL,
drop_type AS B_TECH,connect_dte_sbb AS B_StrtDate,bridger_addr_hse AS B_Node,TenureDays AS B_Tenure, TenureType AS B_TenureType,BundleType AS B_BundleType, 
TOT_MRC AS B_MRC,play_type AS B_play_type
FROM UsefulFields
)

,CustomerBase_EOM AS(
SELECT DISTINCT DATE_TRUNC('Month',FECHA_EXTRACCION) AS Month, sub_acct_no_sbb AS AccountEOM,home_phone_sbb AS E_Phone,email AS E_EMAIL,
drop_type AS E_TECH,connect_dte_sbb AS E_StrtDate,bridger_addr_hse AS E_Node,TenureDays AS E_Tenure, TenureType AS E_TenureType,BundleType AS E_BundleType, 
TOT_MRC AS E_MRC, play_type AS E_play_type
FROM UsefulFields
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

B_Phone,B_EMAIL,B_TECH,B_StrtDate,B_Node,B_Tenure,B_TenureType,B_BundleType,B_MRC,B_play_type,
E_Phone,E_EMAIL,E_TECH,E_StrtDate,E_Node,E_Tenure,E_TenureType,E_BundleType,E_MRC,E_play_type


FROM CustomerBase_BOM b FULL OUTER JOIN CustomerBase_EOM e ON b.AccountBOM = e.AccountEOM AND b.Month = e.Month
)

,MovementsBase AS(
Select Distinct create_dte_ocr,sub_acct_no_ooi,ord_typ from "lcpr_pr_oa"
WHERE ord_typ='V_DISCO' OR ord_typ='NON PAY' AND acct_type='R'
)

-------------------------------------------------------------------------- Movements ----------------------------------------------------------------------------------------
,MainMovementBase AS(
Select Distinct f.*, 
CASE
WHEN B_BundleType LIKE'%1P%' THEN 1
WHEN B_BundleType LIKE'%2P%' THEN 2
WHEN B_BundleType LIKE'%3P%' THEN 3
ELSE NULL END AS B_Num_RGUs,
CASE
WHEN E_BundleType LIKE'%1P%' THEN 1
WHEN E_BundleType LIKE'%2P%' THEN 2
WHEN E_BundleType LIKE'%3P%' THEN 3
ELSE NULL END AS E_Num_RGUs,

CASE
WHEN B_BundleType=E_BundleType THEN '1.Same RGUs'
WHEN (B_BundleType LIKE'%1P%'  AND E_BundleType  IN('%2P%','%3P%')) OR (B_BundleType LIKE'%2P%'  AND E_BundleType  IN('%3P%')) THEN '2.Upsell' 
WHEN (B_BundleType LIKE'%3P%'  AND E_BundleType  IN('%2P%','%1P%')) OR (B_BundleType LIKE'%2P%'  AND E_BundleType  IN('%1P%')) THEN '3.Downsell' 
WHEN (B_BundleType IS NULL AND E_BundleType IS NOT NULL AND DATE_TRUNC('Month',date(E_StrtDate)) <> date('2022-05-01')) THEN '4.Come Back to Life' --- Variabilizar mes
WHEN (B_BundleType IS NULL AND E_BundleType IS NOT NULL AND DATE_TRUNC('Month',date(E_StrtDate)) = date('2022-05-01')) THEN '5.New Customer' --- Variabilizar mes
WHEN ActiveBOM = 1 AND ActiveEOM = 0 THEN '6.Loss'
ELSE NULL END AS MainMovement
FROM fixedcustomerbase f
)

,SpinMovementBase AS(
Select Distinct f.*,
CASE 
WHEN MainMovement='1.Same RGUs' AND (E_MRC - B_MRC) > 0 THEN '1.Up-spin' 
WHEN MainMovement='1.Same RGUs' AND (E_MRC - B_MRC) < 0 THEN '2.Down-spin' 
END AS SpinMovement
FROM MainMovementBase f
)
---------------------------------------------------------------------------- Churn -----------------------------------------------------------------------------------------------

,CustomerBaseWithChurn AS(
Select Distinct f.*, ord_typ 
FROM SpinMovementBase f LEFT JOIN MovementsBase 
ON Fixed_Account=sub_acct_no_ooi AND DATE_TRUNC('Month',date(Fixed_Month))=DATE_TRUNC('Month',date(create_dte_ocr))
)

------------------------------------------------------------------------- Rejoiners -------------------------------------------------------------------------------------------
,InactiveUsersMonth AS (
SELECT DISTINCT Fixed_Month AS ExitMonth, Fixed_Account,DATE_ADD('Month',1,Fixed_Month) AS RejoinerMonth
FROM FixedCustomerBase 
WHERE ActiveBOM=1 AND ActiveEOM=0
)

,RejoinersPopulation AS(
SELECT f.*,RejoinerMonth
,CASE WHEN i.Fixed_Account IS NOT NULL THEN 1 ELSE 0 END AS RejoinerPopFlag,
-- Variabilizar
CASE WHEN date(RejoinerMonth)>=date('2022-05-01') AND date(RejoinerMonth)<=DATE_ADD('Month',1,date('2022-05-01')) THEN 1 ELSE 0 END AS Fixed_PR
FROM FixedCustomerBase f LEFT JOIN InactiveUsersMonth i ON f.Fixed_Account=i.Fixed_Account AND Fixed_Month=ExitMonth
)

,FixedRejoinerFebPopulation AS(
SELECT DISTINCT Fixed_Month,RejoinerPopFlag,Fixed_PR,Fixed_Account,date('2022-05-01') AS Month
FROM RejoinersPopulation
WHERE RejoinerPopFlag=1
AND Fixed_PR=1
AND Fixed_Month!=date('2022-05-01')
GROUP BY 1,2,3,4
)

--,FullFixedBase_Rejoiners AS(
SELECT DISTINCT f.*,Fixed_PR
,CASE WHEN Fixed_PR=1 AND MainMovement='4.Come Back to Life'
THEN 1 ELSE 0 END AS Fixed_Rejoiner
FROM CustomerBaseWithChurn f LEFT JOIN FixedRejoinerFebPopulation r ON f.Fixed_Account=r.Fixed_Account AND f.Fixed_Month=date(r.Month)
--)

