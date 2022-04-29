WITH

FinalTable AS (
    SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Table_DashboardInput_v2`
)

########################################################################### Llamadas tempranas ############################################################################################

,LLAMADAS AS(
    SELECT RIGHT(CONCAT('0000000000',CONTRATO),10) AS CONTRATO, FECHA_APERTURA AS FECHA_LLAMADA, TIQUETE_ID
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-12_CR_TIQUETES_SERVICIO_2021-01_A_2021-11_D`
    WHERE CLASE IS NOT NULL AND MOTIVO IS NOT NULL AND CONTRATO IS NOT NULL
        AND ESTADO <> "ANULADA"
        AND TIPO <> "GESTION COBRO"
        AND MOTIVO <> "LLAMADA  CONSULTA DESINSTALACION"
)

,INSTALACION_CONTRATOS AS (
    SELECT RIGHT(CONCAT('0000000000',ACT_ACCT_CD),10) AS ACT_ACCT_CD, DATE(MIN(ACT_ACCT_INST_DT)) AS INSTALLATION_DT,
    CASE WHEN act_acct_cd IS NOT NULL THEN "Installation" ELSE NULL
    END AS Installations
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
    GROUP BY 1,3
    --HAVING DATE_TRUNC(INSTALLATION_DT, MONTH) >= '2022-02-01' -- solo instalaciones en febrero
)

,CONTRATOS_LLAMADAS AS (
  SELECT *,
    CASE WHEN DATE_DIFF(FECHA_LLAMADA,INSTALLATION_DT,DAY) <= 21 THEN ACT_ACCT_CD ELSE NULL END AS LLAMADA_21D, -- llamadas hasta 21 días después de la instalación
    CASE WHEN DATE_DIFF(FECHA_LLAMADA,INSTALLATION_DT,DAY) <= 49 THEN ACT_ACCT_CD ELSE NULL END AS LLAMADA_7W, -- llamadas hasta 7 semanas (49 días) después de la instalación
    CASE WHEN DATE_DIFF(FECHA_LLAMADA,INSTALLATION_DT,MONTH) BETWEEN 2 AND 6 THEN ACT_ACCT_CD ELSE NULL END AS LLAMADA_2M_6M -- llamadas entre 2 y 6 meses después de la instalación
  FROM INSTALACION_CONTRATOS AS i
  LEFT JOIN LLAMADAS AS l
    ON i.ACT_ACCT_CD = l.CONTRATO
    AND l.FECHA_LLAMADA >= i.INSTALLATION_DT -- el tiquete debe ser después de la instalación
)

,UserCallDistribution AS (
SELECT DISTINCT ACT_ACCT_CD, DATE_TRUNC(INSTALLATION_DT,MONTH) AS InstallationMonth,Installations, COUNT(LLAMADA_21D) AS LLAMADAS_21D, COUNT(LLAMADA_7W) AS Llamadas7semanas, COUNT(LLAMADA_2M_6M) AS Llamadas2a6meses
FROM CONTRATOS_LLAMADAS
GROUP BY 1,2,3
)

,LlamadasMasterTable AS(
  SELECT f.*,Installations, LLAMADAS_21D, Llamadas7semanas, Llamadas2a6meses,  
  FROM FinalTable f LEFT JOIN UserCallDistribution c ON safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string)=safe_cast(c.ACT_ACCT_CD as string) AND safe_cast( InstallationMonth as string)=Month 
)
,LlamadasAjustado AS(
SELECT l.* except(LLAMADAS_21D, Llamadas7semanas, Llamadas2a6meses),
CASE WHEN LLAMADAS_21D =0 THEN NULL ELSE LLAMADAS_21D END AS LLAMADAS_21D,
CASE WHEN Llamadas7semanas=0 THEN NULL ELSE Llamadas7semanas END AS Llamadas7semanas,
CASE WHEN Llamadas2a6meses=0 THEN NULL ELSE Llamadas2a6meses END AS Llamadas2a6meses
FROM LlamadasMasterTable l
)

,Installations AS(
SELECT Distinct(Month), COUNT(distinct Fixed_Account) AS NumInstallations,FROM LlamadasAjustado 
WHERE Installations IS NOT NULL AND (B_FMC_Segment IN('P1_Fixed','P2','P3','P4') OR E_FMC_Segment IN('P1_Fixed','P2','P3','P4'))
GROUP BY 1
)
,NumberOfCalls AS (
SELECT Distinct(Month), COUNT(distinct Fixed_account) AS NumCallers,FROM LlamadasAjustado 
WHERE LLAMADAS_21D IS NOT NULL AND (B_FMC_Segment IN('P1_Fixed','P2','P3','P4') OR E_FMC_Segment IN('P1_Fixed','P2','P3','P4'))
GROUP BY 1
)

SELECT C.*, NumCallers, round(NumCallers/NumInstallations,3) AS PercentageCallers
FROM Installations c LEFT JOIN NumberOfCalls n ON c.Month=n.Month 
ORDER BY Month
