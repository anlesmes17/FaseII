--CREATE OR REPLACE TABLE

--`gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Sprint5_Table_DashboardInput_v2` AS



WITH

FMC_Table AS (
SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Table_DashboardInput_v2`
)

######################################################## KPI Calculations ###########################################################################

------------------------------------------------------- One Time & Repeated Callers -----------------------------------------------------------------

,Interactions_Fields as(
  select distinct *,date_trunc(Fecha_Apertura,Month)  as month,RIGHT(CONCAT('0000000000',CONTRATO),10) AS ContratoInteractions
  From `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220623_CR_TIQUETES_SERVICIO_2021-01_A_2022-05_D`
)

,Last_Interaction as (
  select distinct RIGHT(CONCAT('0000000000',CONTRATO),10) AS last_account,
  first_value(Fecha_Apertura) over(partition by safe_cast(contrato as string),date_trunc(Fecha_Apertura,Month) order by Fecha_Apertura desc) as last_interaction_date
  From `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220623_CR_TIQUETES_SERVICIO_2021-01_A_2022-05_D`
  where clase IS NOT NULL AND Motivo IS NOT NULL AND Contrato IS NOT NULL
        AND ESTADO <> "ANULADA"
        AND TIPO <> "GESTION COBRO"
        AND MOTIVO <> "LLAMADA  CONSULTA DESINSTALACION"
)

,Join_last_interaction as(
  select distinct contratointeractions,Tiquete_id, Fecha_Apertura as interaction_date,date_trunc(last_interaction_date,Month) as InteractionMonth,last_interaction_date,
  date_add(last_interaction_date,Interval -60 day) as window_day
  From interactions_fields w inner join last_interaction l
  on safe_cast(w.contratointeractions as string)=safe_cast(l.last_account as string)
)


,Interactions_Count as(
  select distinct InteractionMonth,Contratointeractions,count(distinct tiquete_id) as Interactions
  From join_last_interaction
  where interaction_date between window_day and last_interaction_date
  group by 1,2
)

,Interactions_tier as(
  select *,
  case when Interactions=1 Then contratointeractions else null end as OneCall,
  case when Interactions=2 Then contratointeractions else null end as TwoCalls,
  case when Interactions>=3 Then contratointeractions else null end as MultipleCalls
  From Interactions_Count
)
,RepeatedCallsMasterTable AS(
  SELECT f.*,OneCall,TwoCalls, MultipleCalls
  FROM FMC_Table f LEFT JOIN Interactions_tier
  ON safe_cast(Contratointeractions as float64)=Fixed_Account AND Month=safe_cast(InteractionMonth as string)
)


------------------------------------------------------------------ Users With Tech Tickets ----------------------------------------------------------------------


,TIQUETES AS(
    SELECT RIGHT(CONCAT('0000000000',CONTRATO),10) AS CONTRATO, FECHA_APERTURA AS FECHA_TIQUETE, DATE_TRUNC(FECHA_APERTURA,MONTH) AS MES_TIQUETE, TIQUETE
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220623_CR_TIQUETES_AVERIA_2021-01_A_2022-05_D`
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
  FROM RepeatedCallsMasterTable f LEFT JOIN OneTicket ON safe_cast(ContratoTicket as string)=safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string) AND Month=safe_cast(TICKET_MONTH as string)
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
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220623_CR_TIQUETES_AVERIA_2021-01_A_2022-05_D`
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
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220623_CR_TIQUETES_AVERIA_2021-01_A_2022-05_D`
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


######################################################## CSV File ########################################################################
select distinct Month,--B_FinalTechFlag, B_FMC_Segment,B_FMCType, E_FinalTechFlag, E_FMC_Segment,E_FMCType,FinalChurnFlag,B_TenureFinalFlag,E_TenureFinalFlag,
 count(distinct fixed_account) as activebase, count(distinct oneCall) as OneCall_Flag,count(distinct TwoCalls) as TwoCalls_Flag,count(distinct MultipleCalls) as MultipleCalls_Flag,
 count(distinct OneTicket) as OneTicket_Flag,count(distinct TwoTickets) as TwoTickets_Flag,count(distinct MultipleTickets) as MultipleTickets_Flag,
 count(distinct FailedInstallations) as FailedInstallations_Flag, round(sum(NumTechTickets)) as TicketDensity_Flag
from NumTiquetesMasterTable
Where finalchurnflag<>"Fixed Churner" AND finalchurnflag<>"Customer Gap" AND finalchurnflag<>"Full Churner" AND finalchurnflag<>"Churn Exception"
Group by 1--,2,3,4,5,6,7,8,9,10
Order by 1 desc, 2,3,4
