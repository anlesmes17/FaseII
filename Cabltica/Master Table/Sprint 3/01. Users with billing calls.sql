WITH

FinalTable AS (
    SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Table_DashboardInput_v2`
)

######################################### Billing Calls #####################################################
,CALLS AS (
SELECT RIGHT(CONCAT('0000000000',CONTRATO),10) AS CONTRATO, DATE_TRUNC(FECHA_APERTURA, MONTH) AS Call_Month, TIQUETE_ID
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-12_CR_TIQUETES_SERVICIO_2021-01_A_2021-11_D`
    WHERE 
        CLASE IS NOT NULL AND MOTIVO IS NOT NULL AND CONTRATO IS NOT NULL
        AND ESTADO <> "ANULADA"
        AND TIPO <> "GESTION COBRO"
        AND MOTIVO NOT IN ("LLAMADA  CONSULTA DESINSTALACION","CONSULTAS DE INSTALACIONES")
        AND MOTIVO = "CONSULTAS DE FACTURACION O COBRO" -- billing
)
,CallsPerUser AS (
    SELECT DISTINCT CONTRATO, Call_Month, Count(DISTINCT TIQUETE_ID) AS NumCalls
    FROM CALLS
    GROUP BY CONTRATO, Call_Month
)

,CallsMasterTable AS (
SELECT F.*, NumCalls
FROM FinalTable f LEFT JOIN CallsPerUser 
ON safe_cast(CONTRATO AS string)=safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) AS string) AND safe_cast(Call_Month as string)=Month
)

,CustomerBase AS(
SELECT Distinct(Month), COUNT(distinct Fixed_ACCOUNT) AS CustomerBase,FROM CallsMasterTable 
WHERE  (B_FMC_Segment IN('P1_Fixed','P2','P3','P4') OR E_FMC_Segment IN('P1_Fixed','P2','P3','P4'))
GROUP BY 1
)
,NumberOfCaller AS (
SELECT Distinct(Month), COUNT(distinct Fixed_ACCOUNT) AS NumCallers,FROM CallsMasterTable 
WHERE NumCalls IS NOT NULL AND (B_FMC_Segment IN('P1_Fixed','P2','P3','P4') OR E_FMC_Segment IN('P1_Fixed','P2','P3','P4'))
GROUP BY 1
)

SELECT C.*, NumCallers, round(NumCallers/CustomerBase,3) AS PercentageCallers
FROM CustomerBase c LEFT JOIN NumberOfCaller n ON c.Month=n.Month 
