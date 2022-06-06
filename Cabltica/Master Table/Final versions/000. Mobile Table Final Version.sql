--CREATE OR REPLACE TABLE

--`gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Mobile_DashboardInput` AS

###################################### Mobile Useful Fields ####################################################
WITH 

MobileFix AS (
  SELECT * except(fec_actcen), 
  CASE WHEN fec_actcen="#N/D" THEN NULL ELSE fec_actcen END AS fec_actcen
  FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220524_cabletica_mobile_DNA` 
)

,MobileUsefulFields AS (
  SELECT DISTINCT DATE_TRUNC(safe_cast(FECHA_PARQUE as date), Month) AS Month,numContrato AS Contrato, ID_CLIENTE, NUM_IDENT, replace(ID_ABONADO,".","") as ID_ABONADO,safe_cast(replace(Renta,",",".") as float64) AS Renta, AGRUPACION_COMUNIDAD, safe_cast(parse_date("%d/%m/%Y",fec_actcen) as string) as fec_actcen
  FROM MobileFix
  WHERE DES_SEGMENTO_CLIENTE <>"Empresas - Empresas" AND DES_SEGMENTO_CLIENTE <>"Empresas - Pymes" 
)

,CustomerBase_BOM AS(
  SELECT DISTINCT  DATE_TRUNC(safe_cast(date_add(safe_cast(Month as date), INTERVAL 1 Month) as date), Month) AS B_Month, Contrato as B_Contrato, ID_CLIENTE as B_IdCliente, NUM_IDENT as B_NumIdent, replace(ID_ABONADO,".","") as B_Mobile_Account,Renta AS B_Renta, AGRUPACION_COMUNIDAD as B_Agrupacion, fec_actcen as B_ActDate,
  FROM MobileUsefulFields
)

,CustomerBase_EOM AS(
  SELECT DISTINCT DATE_TRUNC(safe_cast(safe_cast(Month as date) as date), Month) AS E_Month, Contrato as E_Contrato, ID_CLIENTE as E_IdCliente,NUM_IDENT as E_NumIdent, replace(ID_ABONADO,".","") as E_Mobile_Account,Renta AS E_Renta, AGRUPACION_COMUNIDAD as E_Agrupacion,fec_actcen as E_ActDate,
  FROM MobileUsefulFields
)
/*
,BaseRentaTenure AS (
    SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220411_cabletica_tenure_renta_mobile`
)*/
,BaseMovimientos AS(
    SELECT *,parse_date("%Y%m%d",concat(mes,"01")) as Month FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220516_cabletica_mov_oct_feb` 
)

,MobileCustomerBase AS (
    SELECT DISTINCT
    CASE WHEN (B_Mobile_Account IS NOT NULL AND E_Mobile_Account IS NOT NULL) OR (B_Mobile_Account IS NOT NULL AND E_Mobile_Account IS NULL) THEN B_Month
    WHEN (B_Mobile_Account IS NULL AND E_Mobile_Account IS NOT NULL) THEN E_Month
    END AS Mobile_Month,
    CASE WHEN (B_Mobile_Account IS NOT NULL AND E_Mobile_Account IS NOT NULL) OR (B_Mobile_Account IS NOT NULL AND E_Mobile_Account IS NULL) THEN B_Mobile_Account
    WHEN (B_Mobile_Account IS NULL AND E_Mobile_Account IS NOT NULL) THEN E_Mobile_Account
    END AS Mobile_Account,
    CASE WHEN B_Mobile_Account IS NOT NULL THEN 1 ELSE 0 END AS Mobile_ActiveBOM,
    CASE WHEN E_Mobile_Account IS NOT NULL THEN 1 ELSE 0 END AS Mobile_ActiveEOM,
    B_Contrato,E_Contrato, B_Renta, E_Renta, B_ActDate,E_ActDate
    FROM CustomerBase_BOM b FULL OUTER JOIN CustomerBase_EOM e ON B_Mobile_Account=E_Mobile_Account AND B_Month=E_Month
 
)
/*
,TenureCustomerBase AS (
    SELECT DISTINCT m.*, MIN(MESES_ANTIGUEDAD) AS MESES_ANTIGUEDAD
    FROM MobileCustomerBase m LEFT JOIN BaseRentaTenure 
    ON  replace(NUM_ABONADO,".","")=Mobile_Account
    GROUP BY 1,2,3,4,5,6,7,8,9,10
)*/

,FlagTenureCustomerBase AS (
SELECT DISTINCT *, date_diff(safe_cast(Mobile_Month as date),safe_cast(E_ActDate as date),Month) AS Meses_Antiguedad,
CASE WHEN date_diff(safe_cast(Mobile_Month as date),safe_cast(E_ActDate as date),Month) <6 THEN "Early Tenure"
WHEN date_diff(safe_cast(Mobile_Month as date),safe_cast(E_ActDate as date),Month) >=6 THEN "Late Tenure"
ELSE NULL END AS TenureCustomer

FROM MobileCustomerBase
)

######################################## Main Movements ########################################################

,MainMovements AS (
    SELECT DISTINCT *,CASE 
    WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND(B_Renta=E_Renta) THEN "01.Maintain"
    WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND(B_Renta>E_Renta) THEN "02.Downspin"
    WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND(B_Renta<E_Renta) THEN "03.Upspin"
    WHEN  Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =0 THEN "04.Loss"
    WHEN (Mobile_ActiveBOM=0 OR Mobile_ActiveBOM IS NULL)  AND Mobile_ActiveEOM=1 AND E_ActDate <>"2022-02-01" THEN "05.Come Back To Life"
    WHEN (Mobile_ActiveBOM=0 OR Mobile_ActiveBOM IS NULL)  AND Mobile_ActiveEOM=1 AND E_ActDate ="2022-02-01" THEN "07.New Customer"
    WHEN (B_Renta IS NULL OR E_Renta IS NULL) THEN "03.Upspin"
    ELSE NULL END AS MobileMovementFlag,
    FROM FlagTenureCustomerBase
)

-------------------------------------------- Churners -------------------------------------------------------
,MobileChurners AS (
    SELECT * FROM MainMovements
    WHERE Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=0
)

,ChurnersMovements AS (
    SELECT M.*,TIPO_BAJA AS MobileChurnFlag FROM MobileChurners m LEFT JOIN BaseMovimientos
    ON Mobile_Account=ID_Abonado AND Date_trunc(Mobile_Month,Month)=Date_TRUNC(Month,Month)
)

,CustomerBaseWithChurn AS (
    SELECT DISTINCT m.*, c.MobileChurnFlag
    FROM MainMovements m LEFT JOIN ChurnersMovements c ON m.Mobile_Account=c.Mobile_Account and c.Mobile_Month=m.Mobile_Month
)

----------------------------------------- Rejoiners --------------------------------------------------------------

,InactiveUsersMonth AS (
SELECT DISTINCT Mobile_Month AS ExitMonth, Mobile_Account,DATE_ADD(Mobile_Month, INTERVAL 1 MONTH) AS RejoinerMonth
FROM MobileCustomerBase 
WHERE Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=0
)

,RejoinersPopulation AS(
SELECT f.*,RejoinerMonth
,CASE WHEN i.Mobile_Account IS NOT NULL THEN 1 ELSE 0 END AS RejoinerPopFlag
## Variabilizar
,CASE WHEN RejoinerMonth>='2022-02-01' AND RejoinerMonth<=DATE_ADD('2022-02-01',INTERVAL 1 MONTH) THEN 1 ELSE 0 END AS Mobile_PR
FROM MobileCustomerBase f LEFT JOIN InactiveUsersMonth i ON f.Mobile_Account=i.Mobile_Account AND Mobile_Month=ExitMonth
)

,FixedRejoinerFebPopulation AS(
SELECT DISTINCT Mobile_Month,RejoinerPopFlag,Mobile_PR,Mobile_Account,'2022-02-01' AS Month
FROM RejoinersPopulation
WHERE RejoinerPopFlag=1
AND Mobile_PR=1
AND Mobile_Month<>'2022-02-01'
GROUP BY 1,2,3,4
)

--,FullFixedBase_Rejoiners AS(
SELECT DISTINCT f.*,Mobile_PR
,CASE WHEN Mobile_PR=1 AND MobileMovementFlag="05.Come Back To Life"
THEN f.Mobile_Account ELSE NULL END AS Mobile_Rejoiner
FROM CustomerBaseWithChurn f LEFT JOIN FixedRejoinerFebPopulation r ON f.Mobile_Account=r.Mobile_Account AND f.Mobile_Month=SAFE_CAST(r.Month AS DATE)
--)

