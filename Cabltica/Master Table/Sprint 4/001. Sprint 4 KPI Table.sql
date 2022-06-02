WITH

Sprint3Table AS (
SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Sprint3_Table_DashboardInput_v2`
)

######################################################## KPI Calculations ###########################################################################

------------------------------------------------------- One Time & Repeated Callers -----------------------------------------------------------------

,FECHAS AS (
    SELECT *, LAST_DAY(month_start) AS month_end
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2021-01-01'), CURRENT_DATE(), INTERVAL 1 MONTH)) AS month_start
)

,LLAMADAS AS(
    SELECT RIGHT(CONCAT('0000000000',CONTRATO),10) AS CONTRATO, FECHA_APERTURA AS FECHA_LLAMADA, DATE_TRUNC(FECHA_APERTURA,MONTH) AS MES_LLAMADA, TIQUETE_ID
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-12_CR_TIQUETES_SERVICIO_2021-01_A_2021-11_D`
    WHERE 
        CLASE IS NOT NULL AND MOTIVO IS NOT NULL AND CONTRATO IS NOT NULL
        AND ESTADO <> "ANULADA"
        AND TIPO <> "GESTION COBRO"
        AND MOTIVO <> "LLAMADA  CONSULTA DESINSTALACION"
)

,LLAMADAS_MES AS (
    SELECT CONTRATO, MES_LLAMADA, COUNT(DISTINCT TIQUETE_ID) AS N_LLAMADAS_MES, MAX(FECHA_LLAMADA) AS MAX_APERTURA, DATE_SUB(MAX(FECHA_LLAMADA), INTERVAL 60 DAY) AS MAX_APERTURA_60D
    FROM LLAMADAS
    GROUP BY 1,2
    ORDER BY 2, 3 DESC
)

-- lo que hacemos aquí es buscar por cada mes los usuarios que tengan llamadas en los 60 días anteriores
,LLAMADAS_60D AS (
    SELECT m.*,l.FECHA_LLAMADA AS FECHA_LLAMADA_ANT, l.TIQUETE_ID
    FROM LLAMADAS_MES AS m
    LEFT JOIN LLAMADAS AS l
        ON ((m.CONTRATO = l.CONTRATO) AND (l.FECHA_LLAMADA BETWEEN m.MAX_APERTURA_60D AND m.MAX_APERTURA))
)

,LLAMADAS_GR AS (
    SELECT MES_LLAMADA,CONTRATO,COUNT(DISTINCT TIQUETE_ID) AS N_LLAMADAS_60D,
        CASE -- marca para el número de llamadas en los últimos 60 días
            WHEN COUNT(DISTINCT TIQUETE_ID) = 1 THEN '1'
            WHEN COUNT(DISTINCT TIQUETE_ID) >= 2 THEN '2+'
        ELSE NULL END AS FLAG_LLAMADAS_60D
    FROM LLAMADAS_60D
    GROUP BY 1,2
    ORDER BY 1, 3 DESC
)

,OneCall AS (
    SELECT MES_LLAMADA AS CALL_MONTH, FLAG_LLAMADAS_60D AS CALLS_FLAG, CONTRATO AS ContratoLlamada
    FROM LLAMADAS_GR
    WHERE FLAG_LLAMADAS_60D="1"
    GROUP BY 1,2,3
    ORDER BY 1
)

,MultipleCalls AS (
    SELECT MES_LLAMADA AS CALL_MONTH, FLAG_LLAMADAS_60D AS CALLS_FLAG, CONTRATO AS ContratoLlamada
    FROM LLAMADAS_GR
    WHERE FLAG_LLAMADAS_60D="2+"
    GROUP BY 1,2,3
    ORDER BY 1
)

,OneCallMasterTable AS(
  SELECT f.*,ContratoLlamada AS OneCall
  FROM Sprint3Table f LEFT JOIN OneCall ON safe_cast(ContratoLlamada as string)=safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string) AND Month=safe_cast(CALL_MONTH as string)
)

,MultipleCallsMasterTable AS(
  SELECT f.*,ContratoLlamada AS MultipleCalls
  FROM OneCallMasterTable f LEFT JOIN MultipleCalls ON safe_cast(ContratoLlamada as string)=safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string) AND Month=safe_cast(CALL_MONTH as string)
)

------------------------------------------------------------------ Users With Tech Tickets ----------------------------------------------------------------------


