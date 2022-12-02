--CREATE TABLE IF NOT EXISTS "lla_cco_int_san"."cr_fmc_table"  AS  

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
FROM "cr_ext_parque_temp" inner join "db-analytics-dev"."dna_fixed_cr" 
ON NOM_EMAIL=ACT_CONTACT_MAIL_1
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
WHERE DES_SEGMENTO_CLIENTE <>'Empresas - Empresas' AND DES_
EGMENTO_CLIENTE <>'Empresas - Pymes' 
AND NOM_EMAIL <>'NOTIENE@GMAIL.COM' AND NOM_EMAIL<> 'NOREPORTA@CABLETICA.COM' AND NOM_EMAIL<>'NOREPORTACORREO@CABLETICA.COM'
)

,NEARFMC_MOBILE_EOM AS (
SELECT DISTINCT a.*,b.act_acct_cd AS E_CONTR, E_EMAIL
FROM NEARFMC_MOBILE_BOM a LEFT JOIN EMAIL_EOM b 
ON ID_ABONADO=Mobile_Account AND DATE_ADD('Month',-1,cast(Fecha_Parque as date) )=cast(Mobile_Month as date)
)

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
WHEN (Mobile_Account IS NOT NULL AND Mobile_ActiveEOM=1 AND (ActiveEOM = 0 OR ActiveEOM IS NULL )) AND mobile_churn_type<>'1. Mobile Churner' THEN 'Mobile Only'
END AS E_FMC_Status,f.*,m.*
FROM Fixed_Base f FULL OUTER JOIN NEARFMC_MOBILE_EOM  m 
ON reverse(rpad(substr(reverse(fixed_account),1,10),10,'0'))=cast(reverse(rpad(substr(reverse(fmc_account),1,10),10,'0')) as varchar) AND Fixed_Month=Mobile_Month
)

,CustomerBase_FMC_Tech_Flags AS(
 SELECT t.*,
coalesce(cast(B_BILL_AMT as integer),0) + coalesce(cast(B_Mobile_MRC as integer),0) AS b_total_mrc ,  (coalesce(cast(E_BILL_AMT as integer),0) + coalesce(cast(E_Mobile_MRC as integer),0)) AS e_total_mrc,

CASE  
WHEN (B_FMC_Status = 'Fixed Only' OR B_FMC_Status = 'Soft FMC' OR B_FMC_Status='Near FMC' )  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL) AND B_MIX = '1P' THEN 'Fixed 1P'
WHEN (B_FMC_Status = 'Fixed Only' OR B_FMC_Status = 'Soft FMC' OR B_FMC_Status='Near FMC' )  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL) AND B_MIX = '2P' THEN 'Fixed 2P'
WHEN (B_FMC_Status = 'Fixed Only' OR B_FMC_Status = 'Soft FMC' OR B_FMC_Status='Near FMC' )  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL) AND B_MIX = '3P' THEN 'Fixed 3P'
WHEN (B_FMC_Status = 'Soft FMC' OR B_FMC_Status='Near FMC') AND (ActiveBOM = 0 OR ActiveBOM is null) then 'Mobile Only'
WHEN B_FMC_Status = 'Mobile Only' THEN B_FMC_Status
WHEN (B_FMC_Status='Near FMC' OR  B_FMC_Status='Soft FMC') THEN B_FMC_Status
WHEN final_bom_activeflag=1 and b_numrgus=0 THEN 'Fixed 0P'
END AS B_FMCType,

CASE 
--WHEN Final_EOM_ActiveFlag = 0 AND ((ActiveEOM = 0 AND fixed_churner_type IS NULL) OR (Mobile_ActiveEOM = 0 AND mobile_churn_type is null)) THEN "Customer Gap"
WHEN E_FMC_Status = 'Fixed Only' AND fixed_churner_type IS NOT NULL THEN NULL
WHEN E_FMC_Status = 'Mobile Only' AND mobile_churn_type ='1. Mobile Churner' THEN NULL
WHEN (E_FMC_Status = 'Fixed Only')  AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR(Mobile_ActiveEOM = 1 AND mobile_churn_type IS NOT NULL))  AND E_MIX = '1P' THEN 'Fixed 1P'
WHEN (E_FMC_Status = 'Fixed Only' )  AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR(Mobile_ActiveEOM = 1 AND mobile_churn_type IS NOT NULL)) AND E_MIX = '2P' THEN 'Fixed 2P'
WHEN (E_FMC_Status = 'Fixed Only' )  AND (Mobile_ActiveEOM = 0 OR MOBILE_ACTIVEEOM IS NULL OR(Mobile_ActiveEOM = 1 AND mobile_churn_type IS NOT NULL)) AND E_MIX = '3P' THEN 'Fixed 3P'
WHEN (E_FMC_Status = 'Soft FMC' OR E_FMC_Status = 'Near FMC' OR E_FMC_Status='Mobile Only') AND (ActiveEOM = 0 OR ActiveEOM is null OR (ActiveEOM = 1 AND fixed_churner_type IS NOT NULL)) OR (E_FMC_Status = 'Mobile Only' OR((ActiveEOM is null or activeeom=0) and(Mobile_ActiveEOM=1))) THEN 'Mobile Only'
WHEN (E_FMC_Status='Soft FMC' OR E_FMC_Status='Near FMC') AND (fixed_churner_type IS NULL AND mobile_churn_type<>'1. Mobile Churner' AND ActiveEOM=1 AND Mobile_ActiveEOM=1 ) THEN E_FMC_Status
WHEN final_eom_activeflag=1 and e_numrgus=0 THEN 'Fixed 0P'
END AS E_FMCType
,case when Mobile_ActiveBOM=1 then 1 else 0 end as B_MobileRGUs
,case when Mobile_ActiveEOM=1 then 1 else 0 end as E_MobileRGUs
 FROM FullCustomerBase t
)
 
,CustomerBase_FMCSegments_ChurnFlag AS(
SELECT c.*,
CASE WHEN (B_FMC_Status = 'Fixed Only') OR ((B_FMC_Status = 'Soft FMC' OR B_FMC_Status='Near FMC') AND ACTIVEBOM = 1 AND Mobile_ActiveBOM = 1) THEN B_TechFlag
WHEN B_FMC_Status = 'Mobile Only' OR ((B_FMC_Status = 'Soft FMC' OR B_FMC_Status='Near FMC' OR B_FMC_Status='Undefined FMC') AND (ACTIVEBOM = 0 or ACTIVEBOM IS NULL)) THEN 'Wireless'
END AS B_FinalTechFlag,
CASE
WHEN (E_FMC_Status = 'Fixed Only' AND fixed_churner_type is null) OR ((E_FMC_Status = 'Soft FMC' OR E_FMC_Status='Near FMC') AND ACTIVEEOM = 1 AND Mobile_ActiveEOM = 1 AND fixed_churner_type is null) THEN E_TechFlag
WHEN E_FMC_Status = 'Mobile Only' OR ((E_FMC_Status = 'Soft FMC' OR E_FMC_Status='Near FMC') AND (ACTIVEEOM = 0 OR ActiveEOM IS NULL)) THEN 'Wireless'
END AS E_FinalTechFlag,
case 
when B_FixedTenureSegment='Late Tenure' or B_FixedTenureSegment='Early Tenure' or B_FixedTenureSegment='Mid Tenure' then B_FixedTenureSegment
when B_MobileTenureSegment='Late Tenure' or B_MobileTenureSegment='Early Tenure' or B_MobileTenureSegment='Mid Tenure' then B_MobileTenureSegment
else null end as B_TenureFinalFlag,

case 
when (e_FixedTenureSegment='Late Tenure' or e_FixedTenureSegment='Early Tenure' or e_FixedTenureSegment='Mid Tenure') 
and fixed_churner_type is null then e_FixedTenureSegment
when (e_MobileTenureSegment='Late Tenure' or e_MobileTenureSegment='Early Tenure' or e_MobileTenureSegment='Mid Tenure')
and mobile_churn_type is null then e_MobileTenureSegment
else null end as e_TenureFinalFlag,


/*
CASE WHEN (B_FixedTenureSegment =  'Late Tenure' and B_MobileTenureSegment =  'Late Tenure') OR (B_FixedTenureSegment =  'Late Tenure' and B_MobileTenureSegment is null) or (B_FixedTenureSegment IS NULL and B_MobileTenureSegment =  'Late Tenure') THEN 'Late Tenure'
 WHEN (B_FixedTenureSegment =  'Early Tenure' OR B_MobileTenureSegment =  'Early Tenure') THEN 'Early Tenure'
 END AS B_TenureFinalFlag,
  CASE WHEN (E_FixedTenureSegment =  'Late Tenure' and E_MobileTenureSegment =  'Late Tenure') OR (E_FixedTenureSegment =  'Late Tenure' and E_MobileTenureSegment is null) or (E_FixedTenureSegment IS NULL and E_MobileTenureSegment =  'Late Tenure') THEN 'Late Tenure'
 WHEN (E_FixedTenureSegment =  'Early Tenure' OR E_MobileTenureSegment =  'Early Tenure') THEN 'Early Tenure'
 END AS E_TenureFinalFlag,
*/
CASE
WHEN (B_FMCType = 'Soft FMC' OR B_FMCType = 'Near FMC') AND B_MIX = '1P'  THEN 'P2'
WHEN (B_FMCType  = 'Soft FMC' OR B_FMCType = 'Near FMC') AND B_MIX = '2P' THEN 'P3'
WHEN (B_FMCType  = 'Soft FMC' OR B_FMCType = 'Near FMC') AND B_MIX = '3P' THEN 'P4'

WHEN (B_FMCType  = 'Soft FMC' OR B_FMCType = 'Near FMC') AND B_MIX = '0P' THEN 'P0'

WHEN (B_FMCType  = 'Fixed 1P' OR B_FMCType  = 'Fixed 2P' OR B_FMCType  = 'Fixed 3P') OR ((B_FMCType  = 'Soft FMC' OR B_FMCType='Near FMC') AND(Mobile_ActiveBOM= 0 OR Mobile_ActiveBOM IS NULL)) AND ActiveBOM = 1 THEN 'P1_Fixed'
WHEN (B_FMCType = 'Mobile Only')  OR (B_FMCType  = 'Soft FMC' AND(ActiveBOM= 0 OR ActiveBOM IS NULL)) AND Mobile_ActiveBOM = 1 THEN 'P1_Mobile'
WHEN (B_FMCType  = 'Fixed 0P') OR ((B_FMCType  = 'Soft FMC' OR B_FMCType='Near FMC') AND(Mobile_ActiveBOM= 0 OR Mobile_ActiveBOM IS NULL)) AND ActiveBOM = 1 THEN 'P0_Fixed'

END AS B_FMC_Segment,
CASE 
--WHEN E_FMCType="Customer Gap" THEN "Customer Gap" 
WHEN (E_FMCType = 'Soft FMC' OR E_FMCType='Near FMC') AND (ActiveEOM = 1 and Mobile_ActiveEOM=1) AND E_MIX = '1P' AND (fixed_churner_type IS NULL and mobile_churn_type IS NULL) THEN 'P2'
WHEN (E_FMCType  = 'Soft FMC' OR E_FMCType='Near FMC' OR E_FMCType='Undefined FMC') AND (ActiveEOM = 1 and Mobile_ActiveEOM=1) AND E_MIX = '2P' AND (fixed_churner_type IS NULL and mobile_churn_type IS NULL) THEN 'P3'
WHEN (E_FMCType  = 'Soft FMC' OR E_FMCType='Near FMC' OR E_FMCType='Undefined FMC') AND (ActiveEOM = 1 and Mobile_ActiveEOM=1) AND E_MIX = '3P' AND (fixed_churner_type IS NULL and mobile_churn_type IS NULL) THEN 'P4'
WHEN ((E_FMCType  = 'Fixed 1P' OR E_FMCType  = 'Fixed 2P' OR E_FMCType  = 'Fixed 3P') OR ((E_FMCType  = 'Soft FMC' OR E_FMCType='Near FMC') AND(Mobile_ActiveEOM= 0 OR Mobile_ActiveEOM IS NULL))) AND (ActiveEOM = 1 AND fixed_churner_type IS NULL) THEN 'P1_Fixed'
WHEN ((E_FMCType = 'Mobile Only')  OR (E_FMCType  ='Soft FMC' AND(ActiveEOM= 0 OR ActiveEOM IS NULL))) AND (Mobile_ActiveEOM = 1 and mobile_churn_type IS NULL) THEN 'P1_Mobile'

WHEN (E_FMCType  = 'Fixed 0P') OR ((E_FMCType  = 'Soft FMC' OR E_FMCType='Near FMC') AND(Mobile_ActiveEOM= 0 OR Mobile_ActiveEOM IS NULL)) AND ActiveEOM = 1 THEN 'P0_Fixed'

WHEN (e_FMCType  = 'Soft FMC' OR e_FMCType = 'Near FMC') AND E_MIX = '0P' THEN 'P0'

END AS E_FMC_Segment,case
when (fixed_churner_type is not null and mobile_churn_type is not null) OR (B_FMC_Status = 'Fixed Only' and fixed_churner_type is not null) 
OR (B_FMC_Status = 'Mobile Only' and mobile_churn_type is not null) OR (fixed_churner_type is null and activebom=1 and mobile_activebom=1 AND ((activeeom=0 or activeeom is null) and (Mobile_ActiveEOM=0 or mobile_activeeom Is null))) THEN 'Full Churner'
when (fixed_churner_type is not null and mobile_churn_type is null) then 'Fixed Churner'
when (fixed_churner_type is null and mobile_churn_type is NOT null) then 'Mobile Churner'
when (fixed_churner_type is not null  AND (ActiveBOM IS NULL OR ACTIVEBOM = 0)) OR (mobile_churn_type is not null and (Mobile_ActiveBOM = 0 or Mobile_ActiveBOM IS NULL)) THEN 'Previous churner' -- arreglar los previous churner de una mejor manera
ELSE 'Non Churner' END AS final_churn_flag

,(coalesce(B_NumRGUs,0) + coalesce(B_MobileRGUs,0)) as b_total_rgus
,(coalesce(E_NumRGUs,0) + coalesce(E_MobileRGUs,0)) AS e_total_rgus
,round(e_total_mrc,0) - round(b_total_mrc,0) AS MRC_Change
FROM CustomerBase_FMC_Tech_Flags c
)

,RejoinerColumn AS (
select distinct  f.*,case
when (fixed_rejoiner_type is not null and mobile_rejoiner_type is not null) or 
((fixed_rejoiner_type is not null or mobile_rejoiner_type is not null) and (E_FMCType = 'Soft FMC' OR E_FMCType = 'Near FMC')) then 'FMC Rejoiner'
when fixed_rejoiner_type is not null then 'Fixed Rejoiner'
when mobile_rejoiner_type is not null then 'Mobile Rejoiner'
end as rejoiner_final_flag
FROM CustomerBase_FMCSegments_ChurnFlag f
)

-------------------------------------- Waterfall -------------------------------------

,FullCustomersBase_Flags_Waterfall AS(
SELECT DISTINCT f.*,
CASE 
WHEN final_churn_flag ='Full Churner' THEN 'Total Churner'
WHEN final_churn_flag='Fixed Churner' OR final_churn_flag='Mobile Churner' THEN 'Partial Churner'
WHEN final_churn_flag = 'Non Churner' then null
WHEN final_churn_flag = 'Previous churner' then 'Previous churner'
ELSE null end as Partial_Total_ChurnFlag,
case
when fixed_churner_type='1. Fixed Voluntary Churner' Then 'Voluntary'
when mobile_churn_type='1. Mobile Voluntary Churner'   Then 'Voluntary'
when fixed_churner_type='2. Fixed Involuntary Churner' Then 'Involuntary'
when mobile_churn_type='2. Mobile Involuntary Churner' Then 'Involuntary'
End as churn_type_final_flag
,case
when final_churn_flag<>'Non Churner' then final_churn_flag
when b_total_rgus=_total_rgus and b_total_mrc=e_total_mrc then 'Maintain'
when b_total_rgus<e_total_rgus and b_total_mrc=e_total_mrc then 'Upsell'
when b_total_rgus>e_total_rgus and b_total_mrc=e_total_mrc then 'Downsell'
when b_total_rgus=e_total_rgus and b_total_mrc<e_total_mrc then 'Upspin'
when b_total_rgus=e_total_rgus and b_total_mrc>e_total_mrc then 'Downspin'
when (b_fmc)



CASE 
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_NumRGUs < E_NumRGUs) AND ((Mobile_ActiveBOM=Mobile_ActiveEOM) OR (Mobile_ActiveBOM is null and Mobile_ActiveEOM is null)) THEN 'Upsell'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_NumRGUs > E_NumRGUs) AND ((Mobile_ActiveBOM=Mobile_ActiveEOM) OR (Mobile_ActiveBOM is null and Mobile_ActiveEOM is null) ) THEN 'Downsell'
WHEN ((Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_NumRGUs = E_NumRGUs) AND (b_total_mrc = e_total_mrc)) OR ((Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (ACTIVEBOM IS NULL AND ACTIVEEOM IS NULL) AND (Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=1)) THEN 'Maintain' --??
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_NumRGUs = E_NumRGUs) AND (b_total_mrc> e_total_mrc ) THEN 'Downspin'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (B_NumRGUs = E_NumRGUs) AND (b_total_mrc < e_total_mrc) THEN 'Upspin'
WHEN final_churn_flag='Full Churner' Then 'Full Churner'
WHEN ((Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (final_churn_flag='Fixed Churner') AND ((Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=1) OR (Mobile_ActiveBOM IS NULL AND Mobile_ActiveEOM IS NULL))) OR ((Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag =0) AND (final_churn_flag='Fixed Churner') AND ((Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=1) OR (Mobile_ActiveBOM IS NULL AND Mobile_ActiveEOM IS NULL)))
THEN 'Fixed Churner'
WHEN final_churn_flag='Mobile Churner' Then 'Mobile Churner'
WHEN (Final_BOM_ActiveFlag = 1 and Final_EOM_ActiveFlag = 1) AND (((ActiveBOM=0 AND ActiveEOM=1) AND (Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=1)) OR (ActiveBOM=1 AND ActiveEOM=1) AND (Mobile_ActiveBOM=0 AND Mobile_ActiveEOM=1))  THEN'FMC Packing'
WHEN (Final_BOM_ActiveFlag = 0 and Final_EOM_ActiveFlag = 1)  AND (ActiveBOM=0 AND ActiveEOM=1) AND (Mobile_ActiveBOM=0 AND Mobile_ActiveEOM=1) THEN 'FMC Gross Add'

END AS Waterfall_Flag,
CONCAT(coalesce(B_Plan,''),cast(coalesce(cast(Mobile_ActiveBOM as varchar),'-') as varchar),'') AS B_Plan_Full, 
CONCAT(coalesce(E_Plan,''),cast(coalesce(cast(Mobile_ActiveEOM as varchar),'-') as varchar),'') AS E_Plan_Full 


FROM RejoinerColumn f
)


,Last_Flags as(
select *
,Case when waterfall_flag='Downsell' and MainMovement='Downsell' then 'Voluntary'
      when waterfall_flag='Downsell' and final_churn_flag <> 'Non Churner' then churn_type_final_flag
      when waterfall_flag='Downsell' and mainmovement='Loss' then 'Undefined'
else null end as Downsell_Split
,case when waterfall_flag='Downspin' then 'Voluntary' else null end as Downspin_Split
from FullCustomersBase_Flags_Waterfall
)
select * from last_flags



