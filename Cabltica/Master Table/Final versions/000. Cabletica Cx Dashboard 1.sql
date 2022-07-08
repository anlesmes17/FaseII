WITH 

FMC_Table AS(
  SELECT *,
  "CT" AS opco,"Costa_Rica" AS market,"large" AS marketSize,"Fixed" AS product,"B2C" AS biz_unit,
  Case when MainMovement="New Customer" THEN Fixed_Account Else null end as Gross_Adds,
  Case when Fixed_account is not null then Fixed_Account Else null end as Active_Base
   FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Table_DashboardInput_v2`
)

,Sprint3_KPIs as(-- falta arreglar soft dx y never paid, toca hacer mounting bills
  select distinct Month,sum(activebase) as activebase,sum(sales) as sales,sum(Soft_Dx) as Soft_Dx,sum(NeverPaid) as NeverPaid,--Pendiente incluir mounting bill
  sum(Long_Installs) as Long_Installs,sum(EarlyIssueCall) as EarlyIssueCall,sum(TechCalls) as TechCalls,
  sum(BillClaim) as BilClaim,sum(MRC_Change) as MRC_Change,sum(NoPlan_Changes) as NoPlan_Changes
  From `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Sprint3_Table_DashboardInput_v2`
  where Month<>"2022-06-01" and Month <>"2020-12-01"
  group by 1
)

,S3_CX_KPIs as(
  select distinct Month,"CT" AS opco,"Costa_Rica" AS market,"large" AS market_size,"Fixed" AS product,"B2C" AS biz_unit,
  round(Long_Installs/Sales,4) as breech_cases_installs,round(TechCalls/sales,4) as Early_Tech_Tix,round(EarlyIssueCall/sales,4) as New_Customer_Callers
  From Sprint3_KPIs 
)

,Sprint5_KPIs as(
  select Month,sum(activebase) as activebase, sum(TwoCalls_Flag)+sum(MultipleCalls_Flag) as RepeatedCallers,
  sum(OneTicket_Flag)+sum(TwoTickets_Flag)+sum(TicketDensity_Flag) as numbertickets--validar number tickets
  From `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Sprint5_Table_DashboardInput_v2`
  group by 1
)

,S5_CX_KPIs as(
  select distinct Month,"CT" AS opco,"Costa_Rica" AS market,"large" AS market_size,"Fixed" AS product,"B2C" AS biz_unit,
  sum(RepeatedCallers/activebase) as Repeat_Callers,sum(numbertickets)/sum(activebase) as Tech_Tix_per_100_Acct
  From Sprint5_KPIs
  group by 1
)



############################################################################### New KPIs ##################################################################################

/*
--,payments as(
  select distinct month,opco,market,marketsize,product,biz_unit,'pay' as journey_waypoint,'digital_shift' as facet,'%digital_payments' as kpi_name,
  round(sum(digital)/sum(pymt_cnt)*100,2) as kpi_meas
from( select date_trunc(clearing_date,Month) as Month,"CT" AS opco,"Costa_Rica" AS market,"large" AS marketSize,"Fixed" AS product,"B2C" AS biz_unit,
count(distinct(payment_doc_id)) as pymt_cnt,
case when digital_nondigital = 'Digital' then count(distinct(payment_doc_id)) end as digital
From `dev-fortress-335113.cabletica_ontological_prod_final.payment` 
group by 1,2,3,4,5,6,7
)
*/


########################################################################### All Flags KPIs ################################################################################

