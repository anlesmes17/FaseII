--CREATE OR REPLACE TABLE

--`gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Mobile_DashboardInput` AS

###################################### Mobile Useful Fields ####################################################
WITH 

MobileUsefulFields AS (
  SELECT DISTINCT DATE_TRUNC(safe_cast(FECHA_PARQUE as date), Month) AS Month,numContrato AS Contrato, ID_CLIENTE, NUM_IDENT, replace(ID_ABONADO,".","") as ID_ABONADO,safe_cast(replace(Renta,",",".") as float64) AS Renta, AGRUPACION_COMUNIDAD
  FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220524_cabletica_mobile_DNA` 
  WHERE DES_SEGMENTO_CLIENTE <>"Empresas - Empresas" AND DES_SEGMENTO_CLIENTE <>"Empresas - Pymes" 
)

,CustomerBase_BOM AS(
  SELECT DISTINCT  DATE_TRUNC(safe_cast(date_add(safe_cast(Month as date), INTERVAL 1 Month) as date), Month) AS B_Month, Contrato as B_Contrato, ID_CLIENTE as B_IdCliente, NUM_IDENT as B_NumIdent, replace(ID_ABONADO,".","") as B_Mobile_Account,Renta AS B_Renta, AGRUPACION_COMUNIDAD as B_Agrupacion
  FROM MobileUsefulFields
)

,CustomerBase_EOM AS(
  SELECT DISTINCT DATE_TRUNC(safe_cast(safe_cast(Month as date) as date), Month) AS E_Month, Contrato as E_Contrato, ID_CLIENTE as E_IdCliente,NUM_IDENT as E_NumIdent, replace(ID_ABONADO,".","") as E_Mobile_Account,Renta AS E_Renta, AGRUPACION_COMUNIDAD as E_Agrupacion
  FROM MobileUsefulFields
)

,BaseRentaTenure AS (
    SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220411_cabletica_tenure_renta_mobile`
)
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
    B_Contrato,E_Contrato, B_Renta, E_Renta
    FROM CustomerBase_BOM b FULL OUTER JOIN CustomerBase_EOM e ON B_Mobile_Account=E_Mobile_Account AND B_Month=E_Month
 
)

,TenureCustomerBase AS (
    SELECT DISTINCT m.*, MIN(MESES_ANTIGUEDAD) AS MESES_ANTIGUEDAD
    FROM MobileCustomerBase m LEFT JOIN BaseRentaTenure 
    ON  replace(NUM_ABONADO,".","")=Mobile_Account
    GROUP BY 1,2,3,4,5,6,7,8
)

,FlagTenureCustomerBase AS (
SELECT DISTINCT *,
CASE WHEN MESES_ANTIGUEDAD <6 THEN "Early Tenure"
WHEN MESES_ANTIGUEDAD >=6 THEN "Late Tenure"
ELSE NULL END AS TenureCustomer
FROM TenureCustomerBase
)

######################################## Main Movements ########################################################

,MainMovements AS (
    SELECT DISTINCT *,CASE 
    WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND(B_Renta=E_Renta) THEN "01.Maintain"
    WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND(B_Renta>E_Renta) THEN "02.Downspin"
    WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 AND(B_Renta<E_Renta) THEN "03.Upspin"
    WHEN  Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =0 THEN "04.Loss"
    WHEN Mobile_ActiveBOM=0 AND Mobile_ActiveEOM=1 AND Meses_Antiguedad>1 THEN "05.Come Back To Life"
    WHEN Mobile_ActiveBOM=0 AND Mobile_ActiveEOM=1 AND Meses_Antiguedad<=1 THEN "07.Incomplete Info"
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

--,CustomerBaseWithChurn AS (
    SELECT DISTINCT m.*, c.MobileChurnFlag
    FROM MainMovements m LEFT JOIN ChurnersMovements c ON m.Mobile_Account=c.Mobile_Account and c.Mobile_Month=m.Mobile_Month
--)
