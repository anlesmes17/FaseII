WITH

FinalTable AS (
    SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Table_DashboardInput_v2`
)


####################################################### Outlier Installations ###################################################################################
,tiempo_instalacion AS (
    SELECT NOMBRE_CONTRATO,DATE_TRUNC(SAFE_CAST(FECHA_APERTURA AS DATE),MONTH) AS InstallationMonth,
        TIMESTAMP_DIFF(FECHA_FINALIZACION,FECHA_APERTURA,DAY) AS DIAS_INSTALACION,
        CASE WHEN TIMESTAMP_DIFF(FECHA_FINALIZACION,FECHA_APERTURA,DAY) >= 6 THEN 1 ELSE NULL END AS OUTLIER,
        CASE WHEN NOMBRE_CONTRATO IS NOT NULL THEN "Installation" ELSE NULL END AS Installations
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-03-30_ORDENES_SERVICIO_2022_V2_PRELIMINAR_T`
    WHERE
        TIPO_ORDEN = 'INSTALACION' 
        AND ESTADO = 'FINALIZADA'
        AND TIPO_CLIENTE IN ("PROGRAMA HOGARES CONECTADOS", "RESIDENCIAL", "EMPLEADO")
)
,InstallationsMasterTable AS (
    SELECT f.*, Installations, OUTLIER
    FROM FinalTable f LEFT JOIN tiempo_instalacion ON NOMBRE_CONTRATO=Fixed_Account AND Month=safe_cast(InstallationMonth AS string)
)
,Installations AS(
SELECT Distinct(Month), COUNT(distinct Fixed_Account) AS NumInstallations,FROM InstallationsMasterTable 
WHERE Installations IS NOT NULL AND (B_FMC_Segment IN('P1_Fixed','P2','P3','P4') OR E_FMC_Segment IN('P1_Fixed','P2','P3','P4'))
GROUP BY 1
)
,Outliers AS (
SELECT Distinct(Month), COUNT(distinct Fixed_account) AS NumOutliers,FROM InstallationsMasterTable 
WHERE Outlier IS NOT NULL AND (B_FMC_Segment IN('P1_Fixed','P2','P3','P4') OR E_FMC_Segment IN('P1_Fixed','P2','P3','P4'))
GROUP BY 1
)

SELECT C.*, NumOutliers, round(NumOutliers/NumInstallations,3) AS PercentageOutliers
FROM Installations c LEFT JOIN Outliers n ON c.Month=n.Month
