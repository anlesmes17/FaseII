WITH

FinalTable AS (
    SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Table_DashboardInput_v2`
)


###################################### Truck Rolls After Installations #######################################
,TIQUETES AS(
    SELECT DISTINCT RIGHT(CONCAT('0000000000',CONTRATO),10) AS CONTRATO, FECHA_APERTURA AS FECHA_TIQUETE, DATE_TRUNC(FECHA_APERTURA,MONTH) AS MES_TIQUETE, TIQUETE
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-19_CR_TIQUETES_AVERIAS_2021-01_A_2021-11_D`
    WHERE 
        CLASE IS NOT NULL AND MOTIVO IS NOT NULL AND CONTRATO IS NOT NULL
        AND ESTADO <> "ANULADA"
        AND TIQUETE NOT IN (SELECT DISTINCT TIQUETE FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-19_CR_TIQUETES_AVERIAS_2021-01_A_2021-11_D` WHERE CLIENTE LIKE '%SIN PROBLEMA%')
)

,INSTALACION_CONTRATOS AS (
    SELECT DISTINCT RIGHT(CONCAT('0000000000',ACT_ACCT_CD),10) AS ACT_ACCT_CD, DATE(MIN(ACT_ACCT_INST_DT)) AS INSTALLATION_DT, DATE_TRUNC(DATE(MIN(ACT_ACCT_INST_DT)),MONTH) AS InstallationMonth
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
    GROUP BY 1
    --HAVING DATE_TRUNC(INSTALLATION_DT, MONTH) = '2022-01-01'  -- solo instalaciones en enero
)



,CONTRATOS_TIQUETES AS (
  SELECT DISTINCT * EXCEPT (TIQUETE, FECHA_TIQUETE),
    CASE WHEN DATE_DIFF(FECHA_TIQUETE,INSTALLATION_DT,DAY) BETWEEN 3 AND 49 THEN ACT_ACCT_CD ELSE NULL END AS LLAMADA_7W, -- llamadas hasta 7 semanas (49 días) después de la instalación
    CASE WHEN INSTALLATION_DT IS NOT NULL THEN "Installation" ELSE NULL END AS Installations
  FROM INSTALACION_CONTRATOS AS i
  LEFT JOIN TIQUETES AS l
    ON i.ACT_ACCT_CD = l.CONTRATO
    AND l.FECHA_TIQUETE >= i.INSTALLATION_DT -- el tiquete debe ser después de la instalación
)

,CallsMasterTable AS (
  SELECT DISTINCT f.*, Installations, LLAMADA_7W FROM FinalTable f LEFT JOIN CONTRATOS_TIQUETES c ON RIGHT(CONCAT('0000000000',Fixed_Account),10)=RIGHT(CONCAT('0000000000',c.ACT_ACCT_CD),10) AND Month=safe_cast(InstallationMonth as string)
)

,Installations AS(
SELECT Distinct(Month), COUNT(distinct Fixed_Account) AS NumInstallations,FROM CallsMasterTable 
WHERE Installations IS NOT NULL AND (B_FMC_Segment IN('P1_Fixed','P2','P3','P4') OR E_FMC_Segment IN('P1_Fixed','P2','P3','P4'))
GROUP BY 1
)
,Llamadas AS (
SELECT Distinct(Month), COUNT(distinct Fixed_account) AS NumLlamadas,FROM CallsMasterTable 
WHERE LLAMADA_7W IS NOT NULL AND (B_FMC_Segment IN('P1_Fixed','P2','P3','P4') OR E_FMC_Segment IN('P1_Fixed','P2','P3','P4'))
GROUP BY 1
)

SELECT C.*, NumLlamadas, round(NumLlamadas/NumInstallations,3) AS PercentageTechTickets
FROM Installations c LEFT JOIN Llamadas n ON c.Month=n.Month
ORDER BY Month
