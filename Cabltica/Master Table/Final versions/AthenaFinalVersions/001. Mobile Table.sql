--CREATE TABLE IF NOT EXISTS "lla_cco_int_san"."cr_mobile_table" AS

WITH

MobileUsefulFields as(
Select distinct date_trunc('Month',cast(fecha_parque as date)) as Month, replace(ID_ABONADO,'.','') as ID_ABONADO,cast(Contrato as varchar) as FixedContract,Num_Telefono,Direccion_Correo,des_segmento_cliente,
case 
when (renta like '%#%' or renta like '%/%') then null 
else cast(replace(renta,',','.') as double) end as renta

,CASE WHEN fch_activacion='#N/D' OR fch_activacion='SAMSUNG GALAXY A03 CORE 32GB NEGRO' OR fch_activacion='IPHONE 12 PRO MAX GRAFITO 256GB' 
OR fch_activacion='ARTICULO SIN EQUIPO' 

THEN NULL else date_parse(substring(fch_activacion,1,10),'%m/%d/%Y') END as StartDate 

From "cr_ext_parque_temp"  --limit 10
WHERE DES_SEGMENTO_CLIENTE <>'Empresas - Empresas' AND DES_SEGMENTO_CLIENTE <>'Empresas - Pymes'

)

,CustomerBase_BOM as(
SELECT DISTINCT date_trunc('Month',Month) as B_Month,ID_ABONADO as B_Mobile_Account,FixedContract as B_FixedContract,Renta as B_Mobile_MRC,Num_Telefono as B_NumTelefono,Direccion_correo B_Correo, StartDate as B_StartDate
From MobileUsefulFields
where fixedcontract is not null
)

,CustomerBase_EOM as(
SELECT DISTINCT date_trunc('Month',date_add('Month',-1,Month)) as E_Month,ID_ABONADO as E_Mobile_Account,cast(FixedContract as varchar) as E_FixedContract,Renta as E_Mobile_MRC,Num_Telefono as E_NumTelefono,Direccion_correo as E_Correo, StartDate as E_StartDate
From MobileUsefulFields
)

,MobileCustomerBase as(
SELECT DISTINCT
CASE WHEN (B_Mobile_Account IS NOT NULL AND E_Mobile_Account IS NOT NULL) OR (B_Mobile_Account IS NOT NULL AND 
E_Mobile_Account IS NULL) THEN B_Month
WHEN (B_Mobile_Account IS NULL AND E_Mobile_Account IS NOT NULL) THEN E_Month
END AS Mobile_Month,

CASE WHEN (B_Mobile_Account IS NOT NULL AND E_Mobile_Account IS NOT NULL) OR (B_Mobile_Account IS NOT NULL AND 
E_Mobile_Account IS NULL) THEN B_Mobile_Account
WHEN (B_Mobile_Account IS NULL AND E_Mobile_Account IS NOT NULL) THEN E_Mobile_Account
END AS Mobile_Account,



CASE WHEN B_Mobile_Account IS NOT NULL THEN 1 ELSE 0 END AS Mobile_ActiveBOM,
CASE WHEN E_Mobile_Account IS NOT NULL THEN 1 ELSE 0 END AS Mobile_ActiveEOM,

B_FixedContract,B_NumTelefono,B_Correo,B_Mobile_MRC,B_StartDate,
E_FixedContract,E_NumTelefono,E_Correo,E_Mobile_MRC,E_StartDate
FROM CustomerBase_BOM b FULL OUTER JOIN CustomerBase_EOM e ON B_Mobile_Account=E_Mobile_Account AND 
B_Month=E_Month
)


,FlagTenureCutomerBase as(
SELECT DISTINCT *, date_diff('Month',cast(B_StartDate as date),cast(Mobile_Month as date)) AS Mobile_B_TenureDays,
CASE WHEN date_diff('Month',cast(B_StartDate as date),cast(Mobile_Month as date)) <6 THEN 'Early Tenure'
WHEN date_diff('Month',cast(B_StartDate as date),cast(Mobile_Month as date)) >=6 THEN 'Late Tenure'
ELSE NULL END AS B_MobileTenureSegment,
date_diff('Month',cast(E_StartDate as date),cast(Mobile_Month as date)) AS Mobile_E_TenureDays,
CASE WHEN date_diff('Month',cast(E_StartDate as date),cast(Mobile_Month as date)) <6 THEN 'Early Tenure'
WHEN date_diff('Month',cast(E_StartDate as date),cast(Mobile_Month as date)) >=6 THEN 'Late Tenure'
ELSE NULL END AS E_MobileTenureSegment
From MobileCustomerBase
)


--------------------------------------- Main Movements ----------------------------------------------

,MainMovements as(
SELECT DISTINCT *, 

CASE 
WHEN B_FixedContract IS NOT NULL THEN cast(B_FixedContract as varchar)
WHEN E_FixedContract IS NOT NULL THEN cast(E_FixedContract as varchar)
WHEN Mobile_Account IS NOT NULL THEN cast(Mobile_Account as varchar)
END AS FMC_Account,

CASE
WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND(B_Mobile_MRC=E_Mobile_MRC) THEN '01.Maintain'
WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND(B_Mobile_MRC>E_Mobile_MRC) THEN '02.Downspin'
WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND(B_Mobile_MRC<E_Mobile_MRC) THEN '03.Upspin'
WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =0 THEN '04.Loss'
--WHEN (Mobile_ActiveBOM=0 OR Mobile_ActiveBOM IS NULL)  AND Mobile_ActiveEOM=1 AND E_StartDate <>Mobile_Month THEN '05.Come Back To Life'
--WHEN (Mobile_ActiveBOM=0 OR Mobile_ActiveBOM IS NULL)  AND Mobile_ActiveEOM=1 AND E_StartDate =Mobile_Month THEN '06.New Customer'
WHEN (B_Mobile_MRC IS NULL OR E_Mobile_MRC IS NULL) THEN '07.MRC Gap'
ELSE NULL END AS MobileMovementFlag
From FlagTenureCutomerBase
)

--------------------------------------- Churners ---------------------------------------------------

,MobileChurners as(
SELECT *, '1. Mobile Churner' as MobileChurnFlag
FROM MainMovements
WHERE Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=0
)

,ChurnersMovements as(
SELECT M.*,TIPO_BAJA as MobileChurnType
FROM MobileChurners m LEFT JOIN "cr_ext_mov_temp" 
ON Mobile_Account=cast(ID_Abonado as varchar) AND Date_trunc('Month',Mobile_Month)=Date_TRUNC('Month',cast(dt as date))
)

,CustomerBaseWithChurn AS (
SELECT DISTINCT m.*,
case when mobilechurnflag is not null then MobileChurnFlag
else '2. Mobile NonChurner' end as MobileChurnFlag, 
c.MobileChurnType
FROM MainMovements m LEFT JOIN ChurnersMovements c ON m.Mobile_Account=c.Mobile_Account 
and c.Mobile_Month=
m.Mobile_Month
)

------------------------------------------- Rejoiners ---------------------------------------------------

,InactiveUsersMonth AS (
SELECT DISTINCT Mobile_Month AS ExitMonth, Mobile_Account,DATE_ADD('Month',1, Mobile_Month) AS RejoinerMonth
FROM MobileCustomerBase 
WHERE Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=0
)

,RejoinersPopulation AS(
SELECT f.*,RejoinerMonth
,CASE WHEN i.Mobile_Account IS NOT NULL THEN 1 ELSE 0 END AS RejoinerPopFlag
-- Variabilizar
,CASE WHEN RejoinerMonth>=Mobile_Month AND RejoinerMonth<=DATE_ADD('Month',1, Mobile_Month) THEN 1 ELSE 0 END AS Mobile_PRMonth
FROM MobileCustomerBase f LEFT JOIN InactiveUsersMonth i ON f.Mobile_Account=i.Mobile_Account AND Mobile_Month=ExitMonth
)

,MobileRejoinerPopulation AS(
SELECT DISTINCT Mobile_Month,RejoinerPopFlag,Mobile_PRMonth,Mobile_Account,Cast('2022-02-01' as date) AS Month
FROM RejoinersPopulation
WHERE RejoinerPopFlag=1
AND Mobile_PRMonth=1
AND Mobile_Month<> cast('2022-02-01' as date)
GROUP BY 1,2,3,4
)

,FullMobileBase_Rejoiners AS(
SELECT DISTINCT f.*,Mobile_PRMonth
,CASE WHEN Mobile_PRMonth=1 AND MobileMovementFlag='05.Come Back To Life'
THEN f.Mobile_Account ELSE NULL END AS Mobile_RejoinerMonth
FROM CustomerBaseWithChurn f LEFT JOIN MobileRejoinerPopulation r ON f.Mobile_Account=r.Mobile_Account AND f.Mobile_Month=Month 
)

Select * From FullMobileBase_Rejoiners 
