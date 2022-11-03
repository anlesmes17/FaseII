WITH

FmcTable AS (
SELECT DISTINCT *, 
CASE WHEN B_Plan_Full=E_Plan_Full THEN Fixed_Account ELSE NULL END AS no_plan_change_flag
FROM "lla_cco_int_san"."cr_fmc_table"
) 

,total_installs as(
select 
date_trunc('Month',act_cust_strt_dt) as Sales_Month,
date_trunc('Month',date(act_acct_inst_dt)) as Install_Month,
act_acct_cd
From "db-analytics-dev"."dna_fixed_cr"
)

,installs_fmc_table as(
select f.*,Sales_Month,Install_Month
From FmcTable f left join total_installs b
ON b.act_acct_cd=Fixed_Account --and cast(Month as varchar)=cast(Install_Month as varchar)

)

,sales_fmc_table as(
Select f.*, b.act_acct_cd as monthsale_flag 
From installs_fmc_table f left join "db-analytics-dev"."dna_fixed_cr" b
ON act_acct_cd=Fixed_Account and Month=cast(date_trunc('Month',date_add('Month',1,act_cust_strt_dt)) as date)
)

---------------------------------------- Soft Dx & Never Paid ------------------------------

,FirstBill as(
    Select Distinct act_acct_cd as ContratoFirstBill,Min(Bill_DT_M0) FirstBillEmitted
    From "db-analytics-dev"."dna_fixed_cr"
    group by 1
)

,Prueba as(
    Select distinct Date_Trunc('Month',cast(dt as date)),act_acct_cd,OLDEST_UNPAID_BILL_DT,FI_OUTST_AGE,date_trunc('Month',min(act_cust_strt_dt)) as Sales_Month,dt
    From "db-analytics-dev"."dna_fixed_cr"
    group by 1,2,3,4,6
)

,JoinFirstBill as(
    Select Sales_Month,a.act_acct_cd,FI_OUTST_AGE,dt
    FROM Prueba a inner join FirstBill b
    on ContratoFirstBill=act_acct_cd and FirstBillEmitted=OLDEST_UNPAID_BILL_DT
    order by 2,3,4
)

,MaxOutstAge as(
    Select Distinct Sales_Month,act_acct_cd,Max(FI_OUTST_AGE) as Outstanding_Days,
    Case when Max(FI_OUTST_AGE)>=26 Then act_acct_cd ELSE NULL END AS SoftDx_Flag,
    Case when Max(FI_OUTST_AGE)>=90 Then act_acct_cd ELSE NULL END AS NeverPaid_Flag
    From JoinFirstBill
    group by 1,2
    order by 2,3
)

,SoftDx_MasterTable as(
    Select f.*,SoftDx_Flag 
    From sales_fmc_table f left join MaxOutstAge b ON act_acct_cd=Fixed_Account and cast(b.Sales_Month as date)=Month
)

,NeverPaid_MasterTable as(
    Select f.*,NeverPaid_Flag
    From SoftDx_MasterTable f left join MaxOutstAge b ON act_acct_cd=Fixed_Account and cast(b.Sales_Month as date)=Month
)

----------------------------------------- Early Interactions ----------------------------------------

,Initial_Table_Interactions as(
  select date_trunc('Month',interaction_start_time) as Interaction_Month,account_id as Contrato,interaction_id,min(interaction_start_time) as interaction_start_time
  FROM "interactions_cabletica"
  where interaction_purpose_descrip IS NOT NULL AND account_id IS NOT NULL
  and interaction_status <> 'ANULADA' and interaction_purpose_descrip <> 'GESTION COBRO' and interaction_purpose_descrip <> 'LLAMADA  CONSULTA DESINSTALACION' --AND subarea<>'VENTA VIRTUAL' AND subarea<>"FECHA Y HORA DE VISITA"
  --and subarea<>"FECHA Y HORA DE VISITA WEB"----VALIDAR PURPOSE DESCRIPTION
  group by 1,2,3
)

,Installation_Interactions AS (
    SELECT date_trunc('Month',min(act_cust_strt_dt)) as Sales_Month, act_acct_cd,min(act_cust_strt_dt) as act_cust_strt_dt,
    min(act_acct_inst_dt) as  act_acct_inst_dt,date_trunc('Month',min(act_acct_inst_dt)) as Inst_Month
    FROM "db-analytics-dev"."dna_fixed_cr"
    GROUP BY 2
)

,joint_bases_ei as(
  select t.*,i.sales_month,i.act_cust_strt_dt,i.inst_month,i.act_acct_inst_dt
  From Initial_Table_interactions t left join Installation_Interactions i
  on t.Contrato=act_acct_cd
)

,account_summary_interactions as(
  select Contrato as Account_ID, 
  max(case when date_diff('day',date(act_cust_strt_dt),date(interaction_start_time))<=21 then contrato else null end) as early_interaction,
  Sales_Month,Inst_Month,date_trunc('Month',interaction_start_time) as Interaction_month
  From joint_bases_ei
  group by 1,3,4,5
)

,Early_interaction_MasterTable AS(
  SELECT DISTINCT f.*,early_interaction as EarlyIssue_Flag,Interaction_Month
  FROM NeverPaid_MasterTable f LEFT JOIN account_summary_interactions c 
  ON Fixed_Account=account_id AND Interaction_Month=Month 
)

-------------------------------------- New users early tech tickets -------------------------------

,Initial_Table_Tickets as(
  select date_trunc('Month',interaction_start_time) as Ticket_Month,account_id AS Contrato,interaction_id,min(interaction_start_time) as interaction_start_time
  FROM "interactions_cabletica"
  where interaction_purpose_descrip IS NOT NULL AND account_id IS NOT NULL
  and interaction_status <> 'ANULADA'
  --AND TIQUETE NOT IN (SELECT DISTINCT TIQUETE FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220713_CR_TIQUETES_AVERIA_2021-01_A_2022-06_D` WHERE CLIENTE LIKE '%SIN PROBLEMA%')
  --FALTA DEFINIR CONDICIONES QUE INDIQUEN SI ES UNA AVERÍAAAAA
  group by 1,2,3
)

,Installation_contracts AS (
    SELECT date_trunc('Month',min(act_cust_strt_dt)) as Sales_Month, act_acct_cd,min(act_cust_strt_dt) as act_cust_strt_dt,
    min(act_acct_inst_dt) as  act_acct_inst_dt,date_trunc('Month',min(act_acct_inst_dt)) as Inst_Month
    FROM "db-analytics-dev"."dna_fixed_cr"
    GROUP BY 2
)

,joint_bases_et as(
  select t.*,i.sales_month,i.act_cust_strt_dt,i.inst_month,i.act_acct_inst_dt
  From Initial_Table_Tickets t left join Installation_contracts i
  on t.Contrato=act_acct_cd
)

,account_summary_tickets as(
  select Contrato as Account_ID, 
  max(case when date_diff('week',date(act_cust_strt_dt),date(interaction_start_time))<=7 then contrato else null end) as early_ticket,
  Sales_Month,Inst_Month,date_trunc('Month',interaction_start_time) as ticket_month
  From joint_bases_et
  group by 1,3,4,5
)



,CallsMasterTable AS (
  SELECT DISTINCT f.*, early_ticket as TechCall_Flag FROM Early_interaction_MasterTable f LEFT JOIN account_summary_tickets c 
  ON Fixed_Account=Account_ID AND Month=Ticket_Month
)


--------------------------------------------- Bill Claims ------------------------------------

,CALLS AS (
SELECT account_id AS CONTRATO, DATE_TRUNC('Month',interaction_start_time) AS Call_Month, Interaction_id
    FROM "interactions_cabletica"
    WHERE 
        account_id IS NOT NULL
        AND interaction_status <> 'ANULADA'
        AND interaction_purpose_descrip = 'FACTURACION/COBROS' -- billing
)
,CallsPerUser AS (
    SELECT DISTINCT CONTRATO, Call_Month, Count(DISTINCT interaction_id) AS NumCalls
    FROM CALLS
    GROUP BY CONTRATO, Call_Month
)

,BillingCallsMasterTable AS (
SELECT DISTINCT F.*, CASE WHEN NumCalls IS NOT NULL THEN CONTRATO ELSE NULL END AS BillClaim_Flag
FROM CallsMasterTable f LEFT JOIN CallsPerUser 
ON Contrato=Fixed_Account AND Call_Month=Month
)

-------------------------------------- Bill Shocks -----------------------------------------

,AbsMRC AS (
SELECT *, abs(mrc_change) AS Abs_MRC_Change FROM BillingCallsMasterTable

)
,BillShocks AS (
SELECT DISTINCT *,
CASE
WHEN Abs_MRC_Change>(TOTAL_B_MRC*(.05)) AND B_PLAN=E_PLAN AND no_plan_change_flag is not null THEN Fixed_Account ELSE NULL END AS increase_flag
FROM AbsMRC
)

-------------------------------------- Outlier Installation ---------------------------------

,INSTALACIONES_OUTLIER AS (
    SELECT DISTINCT ACT_ACCT_CD, /*DATE(MIN(ACT_ACCT_INST_DT)) AS INSTALLATION_DT,*/ DATE_TRUNC('Month',MIN(ACT_ACCT_INST_DT)) AS InstallationMonth
    FROM "db-analytics-dev"."dna_fixed_cr"
    GROUP BY 1
)

,tiempo_instalacion AS (
    SELECT account_id AS NOMBRE_CONTRATO,DATE_TRUNC('Month',completed_date) AS InstallationMonth,
        Date_DIFF('Day',order_start_date,completed_date) AS DIAS_INSTALACION,
        CASE WHEN Date_DIFF('Day',order_start_date,completed_date) >= 6 THEN account_id ELSE NULL END AS long_install_flag,
        CASE WHEN account_id IS NOT NULL THEN 'Installation' ELSE NULL END AS Installations
    FROM "db-stage-dev"."so_cr"
    WHERE
        order_type = 'INSTALACION' 
        AND order_status = 'FINALIZADA'
        --AND TIPO_CLIENTE IN ("PROGRAMA HOGARES CONECTADOS", "RESIDENCIAL", "EMPLEADO")
)


,Installations_6_days AS (
    SELECT ACT_ACCT_CD, a.InstallationMonth, b.long_install_flag FROM INSTALACIONES_OUTLIER a LEFT JOIN tiempo_instalacion b
    ON NOMBRE_CONTRATO=ACT_ACCT_CD AND a.InstallationMonth=b.InstallationMonth
)


,OutliersMasterTable AS (
    SELECT DISTINCT f.*, long_install_flag
    FROM BillShocks f LEFT JOIN Installations_6_days b ON b.ACT_ACCT_CD=Fixed_Account AND Month=InstallationMonth
)

--------------------------------------- Mounting Bills --------------------------------

,BillingInfo as(
  Select distinct date_trunc('Month',cast(dt as date)) as Month,act_acct_cd,OLDEST_UNPAID_BILL_DT,Bill_Dt_M0
  From "db-analytics-dev"."dna_fixed_cr"
  where cast(dt as date)=date_trunc('Month',cast(dt as date))
)

,MountingBillJoin as(
  Select distinct b.Month as MountingBillMonth,b.act_acct_cd as MountingBill_Flag
  From BillingInfo a inner join BillingInfo b 
  ON a.act_acct_cd=b.act_acct_cd and a.OLDEST_UNPAID_BILL_DT=b.OLDEST_UNPAID_BILL_DT and a.Month=date_add('Month',-1, b.Month)
  Where b.Bill_Dt_M0 IS NOT NULL
)

,MountingBills_MasterTable as(
  Select f.*,MountingBill_Flag From OutliersMasterTable f left join MountingBillJoin 
  ON Month=MountingBillMonth and Fixed_Account=MountingBill_Flag
)

--------------------------------------- Sales Channel ----------------------------------


--------------------------------------- Grouped table -----------------------------------

select distinct Month,E_FinalTechFlag, E_FMC_Segment,E_FMCType, 
count(distinct fixed_account) as activebase, 
count(distinct monthsale_flag) as Sales, count(distinct SoftDx_Flag) as Soft_Dx, 
count(distinct NeverPaid_Flag) as NeverPaid, count(distinct long_install_flag) as Long_installs, 
count (distinct increase_flag) as MRC_Change, count (distinct no_plan_change_flag) as NoPlan_Changes,
count(distinct EarlyIssue_Flag) as EarlyIssueCall, count(distinct TechCall_Flag) as TechCalls,
count(distinct BillClaim_Flag) as BillClaim,
count(distinct MountingBill_Flag) as MountingBills
--,categoria_canal
,sales_Month,Install_Month
from MountingBills_MasterTable
Where finalchurnflag<>'Fixed Churner' AND finalchurnflag<>'Customer Gap' AND finalchurnflag<>'Full Churner' AND finalchurnflag<>'Churn Exception'
and sales_month>cast('2022-07-01' as date) and Install_month>cast('2022-07-01' as date)
Group by 1,2,3,4,16,17--,18
Order by 1 desc, 2,3,4

