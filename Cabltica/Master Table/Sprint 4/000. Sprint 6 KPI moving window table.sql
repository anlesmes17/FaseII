WITH

Sprint3Table AS (
SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Sprint3_Table_DashboardInput_v2`
)

######################################################## KPI Calculations ###########################################################################

------------------------------------------------------- One Time & Repeated Callers -----------------------------------------------------------------

,LLAMADAS AS(
    SELECT RIGHT(CONCAT('0000000000',CONTRATO),10) AS CONTRATO, FECHA_APERTURA AS FECHA_LLAMADA, DATE_TRUNC(FECHA_APERTURA,MONTH) AS MES_LLAMADA, TIQUETE_ID
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220602_CR_TIQUETES_SERVICIO_2021-01_A_2022-05_D`
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
            WHEN COUNT(DISTINCT TIQUETE_ID) = 2 THEN '2'
            WHEN COUNT(DISTINCT TIQUETE_ID) >= 3 THEN '3+'
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
,TwoCalls AS (
    SELECT MES_LLAMADA AS CALL_MONTH, FLAG_LLAMADAS_60D AS CALLS_FLAG, CONTRATO AS ContratoLlamada
    FROM LLAMADAS_GR
    WHERE FLAG_LLAMADAS_60D="2"
    GROUP BY 1,2,3
    ORDER BY 1
)
,MultipleCalls AS (
    SELECT MES_LLAMADA AS CALL_MONTH, FLAG_LLAMADAS_60D AS CALLS_FLAG, CONTRATO AS ContratoLlamada
    FROM LLAMADAS_GR
    WHERE FLAG_LLAMADAS_60D="3+"
    GROUP BY 1,2,3
    ORDER BY 1
)
,OneCallMasterTable AS(
  SELECT f.*,ContratoLlamada AS OneCall
  FROM Sprint3Table f LEFT JOIN OneCall ON safe_cast(ContratoLlamada as string)=safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string) AND Month=safe_cast(CALL_MONTH as string)
)
,TwoCallsMasterTable AS(
  SELECT f.*,ContratoLlamada AS TwoCalls
  FROM OneCallMasterTable f LEFT JOIN TwoCalls ON safe_cast(ContratoLlamada as string)=safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string) AND Month=safe_cast(CALL_MONTH as string)
)
,MultipleCallsMasterTable AS(
  SELECT f.*,ContratoLlamada AS MultipleCalls
  FROM TwoCallsMasterTable f LEFT JOIN MultipleCalls ON safe_cast(ContratoLlamada as string)=safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string) AND Month=safe_cast(CALL_MONTH as string)
)


------------------------------------------------------------------ Users With Tech Tickets ----------------------------------------------------------------------


,TIQUETES AS(
    SELECT RIGHT(CONCAT('0000000000',CONTRATO),10) AS CONTRATO, FECHA_APERTURA AS FECHA_TIQUETE, DATE_TRUNC(FECHA_APERTURA,MONTH) AS MES_TIQUETE, TIQUETE
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220602_CR_TIQUETES_AVERIA_2021-01_A_2022-05_D`
    WHERE 
        CLASE IS NOT NULL AND MOTIVO IS NOT NULL AND CONTRATO IS NOT NULL
        AND ESTADO <> "ANULADA"
),

TIQUETES_MES AS (
    SELECT CONTRATO, MES_TIQUETE, COUNT(DISTINCT TIQUETE) AS N_TIQUETES_MES, MAX(FECHA_TIQUETE) AS MAX_APERTURA, DATE_SUB(MAX(FECHA_TIQUETE), INTERVAL 60 DAY) AS MAX_APERTURA_60D
    FROM TIQUETES
    GROUP BY 1,2
    ORDER BY 2, 3 DESC
),

-- lo que hacemos aquí es buscar por cada mes los usuarios que tengan llamadas en los 60 días anteriores
TIQUETES_60D AS (
    SELECT m.*,l.FECHA_TIQUETE AS FECHA_TIQUETE_ANT, l.TIQUETE
    FROM TIQUETES_MES AS m
    LEFT JOIN TIQUETES AS l
        ON ((m.CONTRATO = l.CONTRATO) AND (l.FECHA_TIQUETE BETWEEN m.MAX_APERTURA_60D AND m.MAX_APERTURA))
),

TIQUETES_GR AS (
    SELECT MES_TIQUETE,CONTRATO,COUNT(DISTINCT TIQUETE) AS N_TIQUETES_60D,
        CASE -- marca para el número de llamadas en los últimos 60 días
            WHEN COUNT(DISTINCT TIQUETE) = 1 THEN '1'
            WHEN COUNT(DISTINCT TIQUETE) = 2 THEN '2'
            WHEN COUNT(DISTINCT TIQUETE) >= 3 THEN '3+'
        ELSE NULL END AS FLAG_TIQUETES_60D
    FROM TIQUETES_60D
    GROUP BY 1,2
    ORDER BY 1, 3 DESC
)

,OneTicket AS (
    SELECT MES_TIQUETE AS TICKET_MONTH, FLAG_TIQUETES_60D AS TICKETS_FLAG, Contrato AS ContratoTicket
    FROM TIQUETES_GR
    WHERE FLAG_TIQUETES_60D="1"
    GROUP BY 1,2,3
    ORDER BY 1
)

,TwoTickets AS (
    SELECT MES_TIQUETE AS TICKET_MONTH, FLAG_TIQUETES_60D AS TICKETS_FLAG, Contrato AS ContratoTicket
    FROM TIQUETES_GR
    WHERE FLAG_TIQUETES_60D="2"
    GROUP BY 1,2,3
    ORDER BY 1
)

,MultipleTickets AS (
    SELECT MES_TIQUETE AS TICKET_MONTH, FLAG_TIQUETES_60D AS TICKETS_FLAG,Contrato AS ContratoTicket
    FROM TIQUETES_GR
    WHERE FLAG_TIQUETES_60D="3+"
    GROUP BY 1,2,3
    ORDER BY 1
)

,OneTicketMasterTable AS(
  SELECT f.*,ContratoTicket AS OneTicket
  FROM MultipleCallsMasterTable f LEFT JOIN OneTicket ON safe_cast(ContratoTicket as string)=safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string) AND Month=safe_cast(TICKET_MONTH as string)
)

,TwoTicketsMasterTable AS(
  SELECT f.*,ContratoTicket AS TwoTickets
  FROM OneTicketMasterTable f LEFT JOIN TwoTickets ON safe_cast(ContratoTicket as string)=safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string) AND Month=safe_cast(TICKET_MONTH as string)
)

,MultipleTicketsMasterTable AS(
  SELECT f.*,ContratoTicket AS MultipleTickets
  FROM TwoTicketsMasterTable f LEFT JOIN MultipleTickets ON safe_cast(ContratoTicket as string)=safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string) AND Month=safe_cast(TICKET_MONTH as string)
)

--------------------------------------------------------------- Customers With Failed Visits ------------------------------------------------------------------

,FailedInstallations AS (
    SELECT DISTINCT DATE_TRUNC(FECHA_APERTURA, MONTH) AS InstallationMonth, Contrato AS ContratoInstallations
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220602_CR_TIQUETES_AVERIA_2021-01_A_2022-05_D`
    WHERE
        ESTADO IN ('CANCELADA','ANULADA')
        AND TIPO_ATENCION = "TR" -- con esto marcamos que es un truck roll
    GROUP BY 1,2
)

,FailedInstallationsMasterTable AS(
  SELECT DISTINCT f.*, ContratoInstallations AS FailedInstallations
  FROM MultipleTicketsMasterTable f LEFT JOIN FailedInstallations ON safe_cast(RIGHT(CONCAT('0000000000',ContratoInstallations),10) as string)=safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string) AND f.Month=safe_cast(InstallationMonth as string)
)

---------------------------------------------------------------------- Tech Tickets --------------------------------------------------------------------------

,NumTiquetes AS(
    SELECT RIGHT(CONCAT('0000000000',CONTRATO),10) AS CONTRATO, Date_trunc(FECHA_APERTURA, Month) AS TiquetMonth, Count(Distinct TIQUETE) AS NumTechTickets
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220602_CR_TIQUETES_AVERIA_2021-01_A_2022-05_D`
    WHERE 
        CLASE IS NOT NULL AND MOTIVO IS NOT NULL AND CONTRATO IS NOT NULL
        AND ESTADO <> "ANULADA"
        AND MOTIVO <> "LLAMADA  CONSULTA DESINSTALACION"
    GROUP BY 1,2
)

,NumTiquetesMasterTable AS(
    SELECT F.*,NumTechTickets 
    FROM failedinstallationsmastertable f LEFT JOIN NumTiquetes ON CONTRATO=RIGHT(CONCAT('0000000000',Final_Account),10) AND safe_cast(TiquetMonth as string)=Month
)
/*
select * from NumTiquetesMasterTable
where final_account="1155330"
order by month
*/
/*
Select * FROM NumTiquetesMasterTable 
where month="2022-04-01" --and final_account="625304"
order by month
*/

######################################################## CSV File ########################################################################

select distinct Month, B_FinalTechFlag, B_FMC_Segment,B_FMCType, E_FinalTechFlag, E_FMC_Segment,E_FMCType,FinalChurnFlag,B_TenureFinalFlag,E_TenureFinalFlag,
 count(distinct fixed_account) as activebase, count(distinct oneCall) as OneCall_Flag,count(distinct TwoCalls) as TwoCalls_Flag,count(distinct MultipleCalls) as MultipleCalls_Flag,
 count(distinct OneTicket) as OneTicket_Flag,count(distinct TwoTickets) as TwoTickets_Flag,count(distinct MultipleTickets) as MultipleTickets_Flag,
 count(distinct FailedInstallations) as FailedInstallations_Flag, round(sum(NumTechTickets)) as TicketDensity_Flag
from NumTiquetesMasterTable
Where finalchurnflag<>"Fixed Churner" AND finalchurnflag<>"Customer Gap" AND finalchurnflag<>"Full Churner" AND finalchurnflag<>"Churn Exception"
Group by 1,2,3,4,5,6,7,8,9,10
Order by 1 desc--, 2,3,4