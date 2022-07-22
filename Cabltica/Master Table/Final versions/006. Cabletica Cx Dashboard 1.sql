WITH 

FMC_Table AS(
  SELECT *,
  "CT" AS opco,"Costa_Rica" AS market,"large" AS marketSize,"Fixed" AS product,"B2C" AS biz_unit,
  Case when MainMovement="New Customer" THEN Fixed_Account Else null end as Gross_Adds,
  Case when Fixed_account is not null then Fixed_Account Else null end as Active_Base
   FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Table_DashboardInput_v2`

)

,Sprint3_KPIs as(-- falta arreglar soft dx y toca hacer mounting bills
  select distinct Month,sum(activebase) as activebase,sum(sales) as unique_sales,--Pendiente incluir mounting bill
  sum(Long_Installs) as unique_longinstalls,sum(EarlyIssueCall) as unique_earlyinteraction,sum(TechCalls) as unique_earlyticket,
  sum(BillClaim) as unique_billclaim,sum(MRC_Change) as unique_mrcchange,sum(NoPlan_Changes) as noplan
  From `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Sprint3_Table_DashboardInput_v2`
  Where Month<>"2020-12-01" and Month<>"2022-06-01"
  group by 1
)

,S3_CX_KPIs as(
  select distinct Month,"CT" AS opco,"Costa_Rica" AS market,"large" AS MarketSize,"Fixed" AS product,"B2C" AS biz_unit,
  activebase,unique_mrcchange as mrc_change,noplan as noplan_customers,unique_sales,unique_longinstalls,
  unique_earlyticket,unique_earlyinteraction,
  round(unique_mrcchange/noplan,4) as Customers_w_MRC_Changes,--Fix e incluir mounting bill
  round(unique_longinstalls/unique_sales,4) as breech_cases_installs,round(unique_earlyticket/unique_sales,4) as Early_Tech_Tix,
  round(unique_earlyinteraction/unique_sales,4) as New_Customer_Callers
  From Sprint3_KPIs 
)

,Sprint3_Sales_KPIs as(
  select distinct Sales_Month as Month,sum(sales) as unique_sales,sum(Long_Installs) as unique_longinstalls,
  sum(EarlyIssueCall) as unique_earlyinteraction,sum(TechCalls) as unique_earlyticket,sum(Soft_Dx) as unique_softdx
  From `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Sprint3_Table_DashboardInput_v2`
  Where sales_Month>="2021-01-01"
  group by 1
)

,S3_Sales_CX_KPIs as(
  select distinct safe_cast(Month as string) as Month,"CT" AS opco,"Costa_Rica" AS market,"large" AS MarketSize,"Fixed" AS product,"B2C" AS biz_unit,
  unique_sales,unique_longinstalls,unique_earlyticket,unique_earlyinteraction,unique_softdx,
  round(unique_longinstalls/unique_sales,4) as breech_cases_installs,round(unique_earlyticket/unique_sales,4) as Early_Tech_Tix,
  round(unique_earlyinteraction/unique_sales,4) as New_Customer_Callers,round(unique_softdx/unique_sales,4) as New_Sales_to_Soft_Dx
  From Sprint3_Sales_KPIs 
)

,Sprint5_KPIs as(
  select Month,sum(activebase) as activebase, sum(TwoCalls_Flag)+sum(MultipleCalls_Flag) as RepeatedCallers,
  sum(TicketDensity_Flag) as numbertickets
  From `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Sprint5_Table_DashboardInput_v2`
  group by 1
)

,S5_CX_KPIs as(
  select distinct Month,"CT" AS opco,"Costa_Rica" AS market,"large" AS MarketSize,"Fixed" AS product,"B2C" AS biz_unit,
  sum(activebase) as fixed_acc,sum(repeatedcallers) as repeat_callers,sum(numbertickets) as tickets,
  sum(RepeatedCallers/activebase) as Repeated_Callers,sum(numbertickets)/sum(activebase) as Tech_Tix_per_100_Acct
  From Sprint5_KPIs
  group by 1
)

,Additional_KPIs as(
  Select Distinct Month,sum(FixedRGUs) as FixedRGUs,sum(TechCalls) as TechCalls,sum(CareCalls) as CareCalls,sum(BillVariations) as BillVariations
  ,sum(BillingCalls) as BillingCalls,sum(AllBillingCalls) as AllBillingCalls,sum(FTR_Billing) as FTR_Billing
  From `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Additional_Cx_Table_DashboardInput_v2`
  group by 1
)

,Additional_CX_KPIs as(
  Select distinct Month,"CT" AS opco,"Costa_Rica" AS market,"large" AS MarketSize,"Fixed" AS product,"B2C" AS biz_unit,
  sum(FixedRGUs) as unique_FixedRGUs,sum(TechCalls) as unique_TechCalls,sum(CareCalls) as unique_CareCalls,
  sum(BillVariations) as unique_BillVariations,sum(BillingCalls) as unique_BillingCallsBillVariations,
  sum(AllBillingCalls) as unique_allbillingcalls,sum(FTR_Billing) as unique_FTR_Billing
  From Additional_KPIs
  group by 1,2,3,4,5
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
--Prev Calculated
,GrossAdds_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'buy' as journey_waypoint,'Gross_Adds' as kpi_name,
  count(distinct Gross_Adds) as kpi_meas,0 as kpi_num,0 as kpi_den from FMC_Table group by 1,2,3,4,5,6,7,8,9
)

,ActiveBase_Flag1 as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'use' as journey_waypoint,'Active_Base' as kpi_name,
  count(distinct Active_Base) as kpi_meas,0 as kpi_num,0 as kpi_den from FMC_Table group by 1,2,3,4,5,6,7,8,9
)

,ActiveBase_Flag2 as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'support-call' as journey_waypoint,'Active_Base' as kpi_name,
  count(distinct Active_Base) as kpi_meas,0 as kpi_num,0 as kpi_den from FMC_Table group by 1,2,3,4,5,6,7,8,9
)

,TechTickets_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'use' as journey_waypoint,'Tech_Tix_per_100_Acct' as kpi_name,
  round(Tech_Tix_per_100_Acct,4) as kpi_meas,tickets as kpi_num,fixed_acc as kpi_den from S5_CX_KPIs
)

,MRCChanges_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_drivers' as facet,'pay' as journey_waypoint,'%Customers_w_MRC_Changes_5%+_excl_plan' as kpi_name,
round(Customers_w_MRC_Changes,4) as kpi_meas,mrc_change as kpi_num,noplan_customers as kpi_den From S3_CX_KPIs
)

,SalesSoftDx_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'%New_Sales_to_Soft_Dx' as kpi_name,
  round(New_Sales_to_Soft_Dx,4) as kpi_meas,unique_softdx as kpi_num,unique_sales as kpi_den From S3_Sales_CX_KPIs
)

,EarlyIssues_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'buy' as journey_waypoint,'%New_Customer_Callers_2+calls_21days' as kpi_name,
  round(New_Customer_Callers,4) as kpi_meas,unique_earlyinteraction as kpi_num,unique_sales as kpi_den From S3_Sales_CX_KPIs
)

,LongInstall_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'%breech_cases_install_6+days' as kpi_name,
  round(breech_cases_installs,4) as kpi_meas,unique_longinstalls as kpi_num,unique_sales as kpi_den From S3_Sales_CX_KPIs
)

,EarlyTickets_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'get' as journey_waypoint,'%Early_Tech_Tix_-7weeks' as kpi_name,
  round(early_tech_tix,4) as kpi_meas,unique_earlyticket as kpi_num,unique_sales as kpi_den From S3_Sales_CX_KPIs
)

,RepeatedCall_Flag as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'high_risk' as facet,'support-call' as journey_waypoint,'%Repeat_Callers_2+calls' as kpi_name,
  round(Repeated_Callers,4) as kpi_meas,repeat_callers as kpi_num,fixed_acc as kpi_den From S5_CX_KPIs
)

,TechCall1kRGU_Flag as(
  Select Distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,'tech_calls_per_1k_rgu' as kpi_name,
  round(sum(unique_TechCalls)*1000/sum(unique_FixedRGUs),0) as kpi_meas,unique_TechCalls as kpi_num,unique_FixedRGUs as kpi_den From Additional_CX_KPIs
  group by 1,2,3,4,5,6,7,8,11,12
)

,CareCall1kRGU_Flag as(
  Select Distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'support-call' as journey_waypoint,'care_calls_per_1k_rgu' as kpi_name,
  round(sum(unique_CareCalls)*1000/sum(unique_FixedRGUs),0) as kpi_meas,unique_CareCalls as kpi_num,unique_FixedRGUs as kpi_den From Additional_CX_KPIs
  group by 1,2,3,4,5,6,7,8,11,12
)

,BillingCallsPerBillVariation_Flag as(
  Select Distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'contact_intensity' as facet,'pay' as journey_waypoint,'Billing Calls per Bill Variation' as kpi_name,
  round(sum(unique_BillingCallsBillVariations)/sum(unique_BillVariations),3) as kpi_meas,unique_BillingCallsBillVariations as kpi_num,unique_BillVariations as kpi_den
  From Additional_CX_KPIs
  group by 1,2,3,4,5,6,7,8,11,12
)

,FTRBilling_Flag as(
  Select Distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,'effectiveness' as facet,'pay' as journey_waypoint,'%FTR_Billing' as kpi_name,
  sum(unique_FTR_Billing)/sum(unique_allbillingcalls) as kpi_meas,unique_FTR_Billing as kpi_num,unique_allbillingcalls as kpi_den From Additional_CX_KPIs
  group by 1,2,3,4,5,6,7,8,11,12
)

--Pendiente incluir Mounting Bill

############################################################## Join Flags ###########################################################################

,Join_DNA_KPIS as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den
  From( select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den From GrossAdds_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den From ActiveBase_Flag1
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den From ActiveBase_Flag2)
)

,Join_Sprints_KPIs as(
  select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den
  From( select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den From Join_DNA_kpis
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den from TechTickets_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den from MRCChanges_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den from SalesSoftDx_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den from EarlyIssues_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den from LongInstall_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den from EarlyTickets_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den from RepeatedCall_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den from TechCall1kRGU_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den from CareCall1kRGU_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den from BillingCallsPerBillVariation_Flag
  union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den from FTRBilling_Flag
  --union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den from MountingBill_Flag)
  )
)

,Join_New_KPIs as(
select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den
from( select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den from join_sprints_kpis
--union all select Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den from payments
)
)

,FinalTable as(
select distinct Month,Opco,Market,MarketSize,Product,Biz_Unit,facet,journey_waypoint,kpi_name,kpi_meas,kpi_num,kpi_den,extract (year from date(Month)) as ref_year,
extract(month from date(month)) as ref_mo
from Join_New_KPIs

)

Select * From FinalTable
--where ref_year=2022 and ref_mo=2

