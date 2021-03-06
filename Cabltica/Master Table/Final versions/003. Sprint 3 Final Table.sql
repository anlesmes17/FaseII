--CREATE OR REPLACE TABLE

--`gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Sprint3_Table_DashboardInput_v2` AS

WITH

FinalTable AS (
    SELECT DISTINCT *,CONCAT(ifnull(B_Plan,""),ifnull(safe_cast(Mobile_ActiveBOM as string),"")) AS B_PLAN_ADJ, CONCAT(ifnull(E_Plan,""),ifnull(safe_cast(Mobile_ActiveEOM as string),"")) AS E_PLAN_ADJ 
    FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Table_DashboardInput_v2`
) 
,FinalTablePlanAdj AS (
    SELECT DISTINCT *, 
    CASE WHEN B_PLAN_ADJ=E_PLAN_ADJ THEN Fixed_Account ELSE NULL END AS no_plan_change_flag
    FROM FinalTable
)

,install_data as(
  select 
  date_trunc(act_acct_sign_dt,Month) as Sales_Month,
  date_trunc(date(act_acct_inst_dt),Month) as Install_Month,
  act_acct_cd
  From `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-07-15_CR_HISTORIC_CRM_ENE_2021_JUL_2022_BILLING`
)

,installs_fmc_table as(
  select f.*,Sales_Month,Install_Month,
  From FinalTablePlanAdj f left join install_data b
  ON b.act_acct_cd=Fixed_Account --and Month=safe_cast(Install_Month as string)
)

,sales_fmc_table as(
    Select f.*, b.act_acct_cd as monthsale_flag 
    From installs_fmc_table f left join `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-07-15_CR_HISTORIC_CRM_ENE_2021_JUL_2022_BILLING` b
    ON act_acct_cd=Fixed_Account and Month=safe_cast(date_trunc(date_add(act_acct_sign_dt, INTERVAL 1 Month),Month) as string)
)


####################################### Soft Dx & Never Paid ###############################################

,FirstBill as(
    Select Distinct act_acct_cd as ContratoFirstBill,Min(Bill_DT_M0) FirstBillEmitted
    From `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-07-15_CR_HISTORIC_CRM_ENE_2021_JUL_2022_BILLING`
    group by 1
)

,Prueba as(
    Select distinct Date_Trunc(Fecha_Extraccion,Month),act_acct_cd,OLDEST_UNPAID_BILL_DT_NEW,FI_OUTST_AGE_NEW,date_trunc(min(act_acct_sign_dt),Month) as Sales_Month,Fecha_Extraccion
    From `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-07-15_CR_HISTORIC_CRM_ENE_2021_JUL_2022_BILLING`
    group by 1,2,3,4,6
)

,JoinFirstBill as(
    Select Sales_Month,a.act_acct_cd,FI_OUTST_AGE_NEW,Fecha_Extraccion
    FROM Prueba a inner join FirstBill b
    on ContratoFirstBill=act_acct_cd and FirstBillEmitted=OLDEST_UNPAID_BILL_DT_NEW
    order by 2,3,4
)

,MaxOutstAge as(
    Select Distinct Sales_Month,act_acct_cd,Max(FI_OUTST_AGE_NEW) as Outstanding_Days,
    Case when Max(FI_OUTST_AGE_NEW)>=26 Then act_acct_cd ELSE NULL END AS SoftDx_Flag,
    Case when Max(FI_OUTST_AGE_NEW)>=90 Then act_acct_cd ELSE NULL END AS NeverPaid_Flag,
    From JoinFirstBill
    group by 1,2
    order by 2,3
)

,SoftDx_MasterTable as(
    Select f.*,SoftDx_Flag 
    From sales_fmc_table f left join MaxOutstAge b ON act_acct_cd=Fixed_Account and safe_cast(b.Sales_Month as string)=Month
)

,NeverPaid_MasterTable as(
    Select f.*,NeverPaid_Flag
    From SoftDx_MasterTable f left join MaxOutstAge b ON act_acct_cd=Fixed_Account and safe_cast(b.Sales_Month as string)=Month
)



##################################################################### Early Interactions #########################################################

,Initial_Table_Interactions as(
  select date_trunc(Fecha_Apertura,Month) as Interaction_Month,RIGHT(CONCAT('0000000000',CONTRATO),10) AS Contrato,Tiquete_ID,min(Fecha_Apertura) as interaction_start_time
  FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220713_CR_TIQUETES_SERVICIO_2021-01_A_2022-06_D`
  where CLASE IS NOT NULL AND MOTIVO IS NOT NULL AND CONTRATO IS NOT NULL
  and ESTADO <> "ANULADA" and TIPO <> "GESTION COBRO" and MOTIVO <> "LLAMADA  CONSULTA DESINSTALACION" AND subarea<>"VENTA VIRTUAL" AND subarea<>"FECHA Y HORA DE VISITA"
  and subarea<>"FECHA Y HORA DE VISITA WEB"
  group by 1,2,3
)

,Installation_Interactions AS (
    SELECT date_trunc(min(act_acct_sign_dt),Month) as Sales_Month,RIGHT(CONCAT('0000000000',ACT_ACCT_CD),10) AS act_acct_cd,min(act_acct_sign_dt) as act_acct_sign_dt,
    min(act_acct_inst_dt) as  act_acct_inst_dt,date_trunc(min(act_acct_inst_dt),Month) as Inst_Month
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-07-15_CR_HISTORIC_CRM_ENE_2021_JUL_2022_BILLING`
    GROUP BY 2
)

,joint_bases_ei as(
  select t.*,i.sales_month,i.act_acct_sign_dt,i.inst_month,i.act_acct_inst_dt
  From Initial_Table_interactions t left join Installation_Interactions i
  on t.Contrato=act_acct_cd
)

,account_summary_interactions as(
  select Contrato as Account_ID, max(case when date_diff(date(interaction_start_time),date(act_acct_sign_dt),day)<=21 then contrato else null end) as early_interaction,
  Sales_Month,Inst_Month,date_trunc(interaction_start_time,Month) as Interaction_month
  From joint_bases_ei
  group by 1,3,4,5
)

,Early_interaction_MasterTable AS(
  SELECT DISTINCT f.*,early_interaction as EarlyIssue_Flag,Interaction_Month
  FROM NeverPaid_MasterTable f LEFT JOIN account_summary_interactions c 
  ON safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string)=safe_cast(c.Account_ID as string) AND safe_cast( Interaction_Month as string)=Month 
)


################################## New users early tech tickets ###########################################

,Initial_Table_Tickets as(
  select date_trunc(Fecha_Apertura,Month) as Ticket_Month,RIGHT(CONCAT('0000000000',CONTRATO),10) AS Contrato,Tiquete,min(Fecha_Apertura) as interaction_start_time
  FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220713_CR_TIQUETES_AVERIA_2021-01_A_2022-06_D`
  WHERE Clase is not null and Motivo is not null and Contrato is not null and estado <> "ANULADA"
  AND TIQUETE NOT IN (SELECT DISTINCT TIQUETE FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220713_CR_TIQUETES_AVERIA_2021-01_A_2022-06_D` WHERE CLIENTE LIKE '%SIN PROBLEMA%')
  group by 1,2,3
)

,Installation_contracts AS (
    SELECT date_trunc(min(act_acct_sign_dt),Month) as Sales_Month,RIGHT(CONCAT('0000000000',ACT_ACCT_CD),10) AS act_acct_cd,min(act_acct_sign_dt) as act_acct_sign_dt,
    min(act_acct_inst_dt) as  act_acct_inst_dt,date_trunc(min(act_acct_inst_dt),Month) as Inst_Month
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-07-15_CR_HISTORIC_CRM_ENE_2021_JUL_2022_BILLING`
    GROUP BY 2
)

,joint_bases_et as(
  select t.*,i.sales_month,i.act_acct_sign_dt,i.inst_month,i.act_acct_inst_dt
  From Initial_Table_Tickets t left join Installation_contracts i
  on t.Contrato=act_acct_cd
)

,account_summary_tickets as(
  select Contrato as Account_ID, max(case when date_diff(date(interaction_start_time),date(act_acct_sign_dt),week)<=7 then contrato else null end) as early_ticket,
  Sales_Month,Inst_Month,date_trunc(interaction_start_time,Month) as ticket_month
  From joint_bases_et
  group by 1,3,4,5
)



,CallsMasterTable AS (
  SELECT DISTINCT f.*, early_ticket as TechCall_Flag FROM Early_interaction_MasterTable f LEFT JOIN account_summary_tickets c 
  ON RIGHT(CONCAT('0000000000',Fixed_Account),10)=Account_ID AND Month=safe_cast(Ticket_Month as string)

)

############################################# Bill Claims #####################################################

,CALLS AS (
SELECT RIGHT(CONCAT('0000000000',CONTRATO),10) AS CONTRATO, DATE_TRUNC(FECHA_APERTURA, MONTH) AS Call_Month, TIQUETE_ID
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220713_CR_TIQUETES_SERVICIO_2021-01_A_2022-06_D`
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

,BillingCallsMasterTable AS (
SELECT DISTINCT F.*, CASE WHEN NumCalls IS NOT NULL THEN CONTRATO ELSE NULL END AS BillClaim_Flag
FROM CallsMasterTable f LEFT JOIN CallsPerUser 
ON safe_cast(CONTRATO AS string)=safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) AS string) AND safe_cast(Call_Month as string)=Month
)


############################################## Bill shocks #####################################################

,AbsMRC AS (
SELECT *, abs(mrc_change) AS Abs_MRC_Change FROM BillingCallsMasterTable

)
,BillShocks AS (
SELECT DISTINCT *,
CASE
WHEN Abs_MRC_Change>(TOTAL_B_MRC*(.05)) AND B_PLAN=E_PLAN AND no_plan_change_flag is not null THEN Fixed_Account ELSE NULL END AS increase_flag
FROM AbsMRC
)

########################################### Outlier Installations ################################################

,INSTALACIONES_OUTLIER AS (
    SELECT DISTINCT RIGHT(CONCAT('0000000000',ACT_ACCT_CD),10) AS ACT_ACCT_CD, /*DATE(MIN(ACT_ACCT_INST_DT)) AS INSTALLATION_DT,*/ DATE_TRUNC(DATE(MIN(ACT_ACCT_INST_DT)),MONTH) AS InstallationMonth
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-07-15_CR_HISTORIC_CRM_ENE_2021_JUL_2022_BILLING`
    GROUP BY 1
)

,tiempo_instalacion AS (
    SELECT RIGHT(CONCAT('0000000000',NOMBRE_CONTRATO),10) AS NOMBRE_CONTRATO,DATE_TRUNC(SAFE_CAST(FECHA_FINALIZACION AS DATE),MONTH) AS InstallationMonth,
        TIMESTAMP_DIFF(FECHA_FINALIZACION,FECHA_APERTURA,DAY) AS DIAS_INSTALACION,
        CASE WHEN TIMESTAMP_DIFF(FECHA_FINALIZACION,FECHA_APERTURA,DAY) >= 6 THEN NOMBRE_CONTRATO ELSE NULL END AS long_install_flag,
        CASE WHEN NOMBRE_CONTRATO IS NOT NULL THEN "Installation" ELSE NULL END AS Installations
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220623_CR_ORDENES_SERVICIO_2021-01_A_2022-05_D`
    WHERE
        TIPO_ORDEN = 'INSTALACION' 
        AND ESTADO = 'FINALIZADA'
        AND TIPO_CLIENTE IN ("PROGRAMA HOGARES CONECTADOS", "RESIDENCIAL", "EMPLEADO")
)

,Installations_6_days AS (
    SELECT ACT_ACCT_CD, a.InstallationMonth, b.long_install_flag FROM INSTALACIONES_OUTLIER a LEFT JOIN tiempo_instalacion b
    ON safe_cast(NOMBRE_CONTRATO as string)=safe_cast(ACT_ACCT_CD as string) AND a.InstallationMonth=b.InstallationMonth
)


,OutliersMasterTable AS (
    SELECT DISTINCT f.*, long_install_flag
    FROM BillShocks f LEFT JOIN Installations_6_days b ON safe_cast(b.ACT_ACCT_CD as string)=RIGHT(CONCAT('0000000000',Fixed_Account),10) AND Month=safe_cast(InstallationMonth AS string)
)


######################################## Mounting Bills #####################################################

,BillingInfo as(
  Select distinct date_trunc(Fecha_extraccion,Month) as Month,act_acct_cd,OLDEST_UNPAID_BILL_DT_NEW,Bill_Dt_M0
  From `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-07-15_CR_HISTORIC_CRM_ENE_2021_JUL_2022_BILLING`
  where fecha_extraccion=date_trunc(fecha_extraccion,Month)
)

,MountingBillJoin as(
  Select distinct b.Month as MountingBillMonth,b.act_acct_cd as MountingBill_Flag
  From BillingInfo a inner join BillingInfo b 
  ON a.act_acct_cd=b.act_acct_cd and a.OLDEST_UNPAID_BILL_DT_NEW=b.OLDEST_UNPAID_BILL_DT_NEW and a.Month=date_add(b.Month, INTERVAL -1 MONTH)
  Where b.Bill_Dt_M0 IS NOT NULL
)

,MountingBills_MasterTable as(
  Select f.*,MountingBill_Flag From OutliersMasterTable f left join MountingBillJoin 
  ON Month=safe_cast(MountingBillMonth as string) and Fixed_Account=MountingBill_Flag
)


######################################## Sales Channel ######################################################

,SalesChannel as (
SELECT distinct contrato,Categoria_Canal,subcanal_venta, case 
WHEN formato_fecha like "%2022" then parse_date("%d/%m/%Y",formato_fecha) else safe_cast(formato_fecha as date) end as FechaAdj
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220510-Altas_ene_2021_abr_2022` 
group by 1,2,3,formato_fecha
)
,FechaFin AS (
    Select distinct  *, min(FechaAdj) AS FechaFinal From SalesChannel
    group by 1,2,3,4

)

,SalesChannelsInstallations as (
select distinct Fixed_Account, contrato,Install_Month, Categoria_Canal, subcanal_venta,
from installs_fmc_table left join FechaFin
on RIGHT(CONCAT('0000000000',CONTRATO),10)=RIGHT(CONCAT('0000000000',Fixed_Account),10) and Install_Month=date_trunc(FechaFinal,month)

)
,ChannelsMasterTable AS (
    select distinct f.*, categoria_canal, subcanal_venta, from MountingBills_MasterTable f left join SalesChannelsInstallations s
    on RIGHT(CONCAT('0000000000',CONTRATO),10)=RIGHT(CONCAT('0000000000',f.Fixed_Account),10) AND safe_cast(s.Install_Month as string)=Month
)
,ChannelAndSubchannel AS (
  SELECT distinct *, CASE
  WHEN Subcanal_venta="INHOUSE FORMULARIO" OR Subcanal_venta="INHOUSE CHAT" OR Subcanal_venta="INHOUSE WP" OR Subcanal_venta="ITS FORMULARIOS" OR
  Subcanal_venta="ITS WP" OR Subcanal_venta="ITS WP MOVISTAR" OR Subcanal_venta="ITS CHAT" OR Subcanal_venta="INHOUSE WP MOVISTAR" Then "Digital"
  WHEN Subcanal_venta="OUTBOUND CAC" OR Subcanal_venta="OUTBOUND TELEVENTAS" OR Subcanal_venta="OUTBOUND SUCURSALES" OR Subcanal_venta="ITS OUTBOUND" OR Subcanal_venta="ITS CRIANZA" OR Subcanal_venta="ITS CORREO" 
  OR Subcanal_venta="OUTBOUND ALBUFERA" OR Subcanal_venta="OUTBOUND DATA 506" OR Subcanal_venta="OUTBOUND TICO CEL" OR Subcanal_venta="OUTBOUND TEL MOVIL" OR Subcanal_venta="OUTBOUND RED ALO" THEN "Televentas-Outbound"
  WHEN Subcanal_venta="INHOUSE WP" OR Subcanal_venta="INHOUSE CHAT" OR Subcanal_venta="INHOUSE FORMULARIO" OR Subcanal_venta="TELE VENTAS" OR Subcanal_venta="ITS FORMULARIOS" OR Subcanal_venta="ITS INBOUND" THEN "Televentas-Inbound"
  WHEN Subcanal_venta="VENTAS RESIDENCIALES" THEN "D2D-Local"
  WHEN Subcanal_venta="CTVS" OR Subcanal_venta="" OR Subcanal_venta="CAMBIOS POSITIVOS" OR Subcanal_venta="UPGRADE SOLUTIONS" OR Subcanal_venta="CRC EN LINEA SA" OR Subcanal_venta="CONECTIVIDAD CR" THEN "D2D-Third Party"
  Else categoria_canal END AS SalesChannelAdjusted FROM ChannelsMasterTable
)


,FinalSalesChannel AS(
select DISTINCT * except(categoria_canal, subcanal_venta), CASE
WHEN SalesChannelAdjusted="Digital"  THEN "Digital"
WHEN SalesChannelAdjusted="Televentas-Outbound"  THEN "Televentas-Outbound"
WHEN SalesChannelAdjusted="Televentas-Inbound"  THEN "Televentas-Inbound"
WHEN SalesChannelAdjusted="D2D-Local"  THEN "D2D-Local"
WHEN SalesChannelAdjusted="D2D-Third Party"  THEN "D2D-Third Party"
WHEN SalesChannelAdjusted="Agentes Autorizados" OR SalesChannelAdjusted="Ventas Residenciales" THEN "D2D"
WHEN SalesChannelAdjusted="Televentas" OR SalesChannelAdjusted="ITS" THEN "Telesales"
WHEN SalesChannelAdjusted="*No Definido*" OR SalesChannelAdjusted="Sin datos" THEN "Undefined/ No Data"
WHEN SalesChannelAdjusted="Oficina" THEN "Retail"
WHEN SalesChannelAdjusted="NETCOM" OR SalesChannelAdjusted="Hoteles/Condominios" OR SalesChannelAdjusted="Ventas Empresariales" THEN "Other"
ELSE NULL END AS Categoria_canal
from ChannelAndSubchannel
order by Month

)

################################################# Excel Table ###############################################

select distinct Month,E_FinalTechFlag, E_FMC_Segment,E_FMCType, 
count(distinct fixed_account) as activebase, 
count(distinct monthsale_flag) as Sales, count(distinct SoftDx_Flag) as Soft_Dx, 
count(distinct NeverPaid_Flag) as NeverPaid, count(distinct long_install_flag) as Long_installs, 
count (distinct increase_flag) as MRC_Change, count (distinct no_plan_change_flag) as NoPlan_Changes,
count(distinct EarlyIssue_Flag) as EarlyIssueCall, count(distinct TechCall_Flag) as TechCalls,
count(distinct BillClaim_Flag) as BillClaim,
count(distinct MountingBill_Flag) as MountingBills,categoria_canal,sales_Month,Install_Month
from FinalSalesChannel
Where finalchurnflag<>"Fixed Churner" AND finalchurnflag<>"Customer Gap" AND finalchurnflag<>"Full Churner" AND finalchurnflag<>"Churn Exception"
Group by 1,2,3,4,16,17,18
Order by 1 desc, 2,3,4
