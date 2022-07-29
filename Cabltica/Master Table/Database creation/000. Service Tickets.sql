--CREATE OR REPLACE 
--TABLE `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220713_CR_TIQUETES_SERVICIO_2021-01_A_2022-06_D` AS

WITH TIQUETESSERVICIO_2022 AS(
    SELECT SITE
    , CLASE
    , MOTIVO
    , CANAL_DE_ATENCI__N AS CANAL_DE_ATENCION
    , TIPO_DE_GESTION
    , N___de_tiquete AS TIQUETE_ID
    , DATE(APERTURA) AS FECHA_APERTURA
    , ESTADO
    , __REA AS AREA
    , SUB__REA AS SUBAREA
    , CONTRATO
    , PROVINCIA
    , CANT__N AS CANTON
    , DISTRITO
    , BARRIO
    , DATE(FECHA_DE_FINALIZACI__N) AS FECHA_FINALIZACION
    , ESTRATEGIA_APLICADA
    , ANTIG__EDAD AS ANTIGUEDAD
    , SERVICIOS_AFECTADOS AS SERVICIO_AFECTADO
    , SERVICIO_NO_RETENIDO
    , SOLUCI__N AS SOLUCION
    , TIPO

 FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220602_CR_TIQUETES_SERVICIO_PREL` 
 WHERE EXTRACT (YEAR FROM Apertura) = 2022 OR DATE_TRUNC(Apertura,MONTH)="2021-12-01 00:00:00 UTC"
)

, TIQUETESSERVICIO_2021 AS(
 
 SELECT *
 FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-12_CR_TIQUETES_SERVICIO_2021-01_A_2021-11_D` 
 WHERE EXTRACT(YEAR FROM FECHA_APERTURA) = 2021

)

, TIQUETESMAYO AS(
  SELECT SITE
    , CLASE
    , MOTIVO
    , CANAL_DE_ATENCI__N AS CANAL_DE_ATENCION
    , TIPO_DE_GESTION
    , N___de_tiquete AS TIQUETE_ID
    , DATE(APERTURA) AS FECHA_APERTURA
    , ESTADO
    , __REA AS AREA
    , SUB__REA AS SUBAREA
    , CONTRATO
    , PROVINCIA
    , CANT__N AS CANTON
    , DISTRITO
    , BARRIO
    , DATE(FECHA_DE_FINALIZACI__N) AS FECHA_FINALIZACION
    , ESTRATEGIA_APLICADA
    , ANTIG__EDAD AS ANTIGUEDAD
    , SERVICIOS_AFECTADOS AS SERVICIO_AFECTADO
    , SERVICIO_NO_RETENIDO
    , SOLUCI__N AS SOLUCION
    , TIPO
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220623_tiquetes_serv_mayo`
)
, TIQUETESjunio AS(
  SELECT SITE
    , CLASE
    , MOTIVO
    , CANAL_DE_ATENCI__N AS CANAL_DE_ATENCION
    , TIPO_DE_GESTION
    , N___de_tiquete AS TIQUETE_ID
    , DATE(APERTURA) AS FECHA_APERTURA
    , ESTADO
    , __REA AS AREA
    , SUB__REA AS SUBAREA
    , CONTRATO
    , PROVINCIA
    , CANT__N AS CANTON
    , DISTRITO
    , BARRIO
    , DATE(FECHA_DE_FINALIZACI__N) AS FECHA_FINALIZACION
    , ESTRATEGIA_APLICADA
    , ANTIG__EDAD AS ANTIGUEDAD
    , SERVICIOS_AFECTADOS AS SERVICIO_AFECTADO
    , SERVICIO_NO_RETENIDO
    , SOLUCI__N AS SOLUCION
    , TIPO
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220713_ServiceTickets`
)

--,union_tables as(
SELECT * FROM
(SELECT * FROM TIQUETESSERVICIO_2021
UNION ALL
SELECT * FROM TIQUETESSERVICIO_2022
UNION ALL
SELECT * FROM TIQUETESMAYO
UNION ALL
SELECT * FROM TIQUETESjunio)
order by  FECHA_FINALIZACION
--)
