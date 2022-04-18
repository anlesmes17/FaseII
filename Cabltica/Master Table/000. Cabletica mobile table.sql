CREATE OR REPLACE TABLE

`gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Mobile_DashboardInput` AS

###################################### Mobile Useful Fields ####################################################
WITH 

MobileUsefulFields_BOM AS (
    SELECT DISTINCT Date_Trunc(FECHA_PARQUE, month) AS Month, Contrato, ID_CLIENTE, NUM_IDENT, replace(ID_ABONADO,".","") as ID_ABONADO, AGRUPACION_COMUNIDAD
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220411_cabletica_fmc_febrero` 
    WHERE DES_SEGMENTO_CLIENTE <>"Empresas - Empresas" AND DES_SEGMENTO_CLIENTE <>"Empresas - Pymes" --AND (DES_USO ="MOVIL CONTRATO" OR DES_USO="DATOS MOVIL POSPAGO (UMTS/DATACARD)" OR DES_USO="DATOS MOVIL POSPAGO (UMTS/DATACARD)") AND (AGRUPACION_COMUNIDAD<>"NULL" AND AGRUPACION_COMUNIDAD<>"OTROS" AND AGRUPACION_COMUNIDAD<>"B2H")
)
,MobileUsefulFields_EOM AS (
    SELECT DISTINCT Date_Trunc(FECHA_PARQUE, month) AS Month,Contrato, ID_CLIENTE, NUM_IDENT, replace(ID_ABONADO,".","") as ID_ABONADO, AGRUPACION_COMUNIDAD
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220411_cabletica_fmc_marzo`
    WHERE DES_SEGMENTO_CLIENTE <>"Empresas - Empresas" AND DES_SEGMENTO_CLIENTE <>"Empresas - Pymes" --AND (DES_USO ="MOVIL CONTRATO" OR DES_USO="DATOS MOVIL POSPAGO (UMTS/DATACARD)" OR DES_USO="DATOS MOVIL POSPAGO (UMTS/DATACARD)") AND (AGRUPACION_COMUNIDAD<>"NULL" AND AGRUPACION_COMUNIDAD<>"OTROS" AND AGRUPACION_COMUNIDAD<>"B2H")
)
,CR_Mobile_UsefulFields AS(
SELECT  DISTINCT *
from (SELECT * from MobileUsefulFields_BOM b 
      UNION ALL
      SELECT * from MobileUsefulFields_EOM e)
)
,BaseRentaTenure AS (
    SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220411_cabletica_tenure_renta_mobile`
)
,BaseMovimientos AS(
    SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.20220404_cabletica_movimientos_fmc`
)



,MobileCustomerBase_BOM AS(
    SELECT DISTINCT  DATE_TRUNC(Month,Month) AS Month, ID_ABONADO AS B_Mobile_Account, Contrato as B_Contrato
    from CR_Mobile_UsefulFields 
)

,MobileCustomerBase_EOM AS(
    SELECT DISTINCT  DATE_TRUNC(DATE_SUB(Month, INTERVAL 1 MONTH),MONTH) AS Month, ID_ABONADO AS E_Mobile_Account, Contrato as E_Contrato
    from CR_Mobile_UsefulFields 
)
,MobileCustomerBase AS (
    SELECT DISTINCT
    CASE WHEN (B_Mobile_Account IS NOT NULL AND E_Mobile_Account IS NOT NULL) OR (B_Mobile_Account IS NOT NULL AND E_Mobile_Account IS NULL) THEN b.Month
    WHEN (B_Mobile_Account IS NULL AND E_Mobile_Account IS NOT NULL) THEN e.Month
    END AS Mobile_Month,
    CASE WHEN (B_Mobile_Account IS NOT NULL AND E_Mobile_Account IS NOT NULL) OR (B_Mobile_Account IS NOT NULL AND E_Mobile_Account IS NULL) THEN B_Mobile_Account
    WHEN (B_Mobile_Account IS NULL AND E_Mobile_Account IS NOT NULL) THEN E_Mobile_Account
    END AS Mobile_Account,
    CASE WHEN B_Mobile_Account IS NOT NULL THEN 1 ELSE 0 END AS Mobile_ActiveBOM,
    CASE WHEN E_Mobile_Account IS NOT NULL THEN 1 ELSE 0 END AS Mobile_ActiveEOM,
    B_Contrato,
    E_Contrato
    FROM MobileCustomerBase_BOM b FULL OUTER JOIN MobileCustomerBase_EOM e ON B_Mobile_Account=E_Mobile_Account AND b.Month=e.Month
 
)
/*SELECT DISTINCT MOBILE_MONTH, COUNT(DISTINCT Mobile_Account) FROM MobileCustomerBase
--WHERE Mobile_ActiveBOM=1 AND Mobile_ActiveEOM=1
GROUP BY MOBILE_Month*/


,TenureCustomerBase AS (
    SELECT DISTINCT m.*, MIN(MESES_ANTIGUEDAD) AS MESES_ANTIGUEDAD, RENTA
    FROM MobileCustomerBase m LEFT JOIN BaseRentaTenure 
    ON NUM_ABONADO=Mobile_Account
    GROUP BY 1,2,3,4,5,6,8
)

,FlagTenureCustomerBase AS (
SELECT DISTINCT *,
CASE WHEN MESES_ANTIGUEDAD <6 THEN "Early Tenure"
WHEN MESES_ANTIGUEDAD >=6 THEN "Late Tenure"
ELSE NULL END AS TenureCustomer
FROM TenureCustomerBase
)

######################################## Main Movements ########################################################

,MAINMOVEMENTS AS (
    SELECT DISTINCT *,
    CASE WHEN Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =1 THEN "1.Maintain"
    WHEN  Mobile_ActiveBOM =1 AND Mobile_ActiveEOM =0 THEN "2.Loss"
    WHEN Mobile_ActiveBOM=0 AND Mobile_ActiveEOM=1 THEN "3.Gross Add/ rejoiner"
    ELSE "3.NULL" END AS MobileMovementFlag,
    FROM MobileCustomerBase
)


----------------------------------------------Churners--------------------------------------------------------

,MobileChurners AS (
Select DISTINCT e.ID_ABONADO AS Account_EOM, b.ID_ABONADO As Account_BOM
FROM MobileUsefulFields_BOM b LEFT JOIN MobileUsefulFields_EOM e ON e.ID_ABONADO=b.ID_ABONADO
)
,AllChurners AS (
    SELECT Account_BOM, Account_EOM, CASE
    WHEN Account_EOM IS NULL THEN "Churner"
    ELSE "Non Churners" END AS Churners
    FROM MobileChurners
    WHERE Account_EOM IS NULL
)


,ChurnerClassification AS (
SELECT DISTINCT Account_BOM, Churners, TIPO_BAJA
FROM AllChurners m Left Join BaseMovimientos b ON Account_BOM= b.ID_ABONADO
)
--,CustomerBaseWithChurn AS (
    SELECT DISTINCT m.*, c.Churners, c.TIPO_BAJA
    FROM MobileCustomerBase m LEFT JOIN ChurnerClassification c ON m.Mobile_Account=c.Account_BOM
