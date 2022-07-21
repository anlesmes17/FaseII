--CREATE OR REPLACE TABLE

--`gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Additional_Cx_Table_DashboardInput_v2` AS


WITH

fmc_table as(
  Select *,
  CONCAT(ifnull(B_Plan,""),ifnull(safe_cast(Mobile_ActiveBOM as string),"")) AS B_PLAN_ADJ, CONCAT(ifnull(E_Plan,""),ifnull(safe_cast(Mobile_ActiveEOM as string),"")) AS         
  E_PLAN_ADJ  From `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-07-14_Cabletica_FMC_DashboardInput`
)

,FinalTablePlanAdj AS (
  SELECT DISTINCT *, 
  CASE WHEN B_PLAN_ADJ=E_PLAN_ADJ THEN Fixed_Account ELSE NULL END AS no_plan_change_flag
  FROM fmc_table
)

################################################################ Tech calls per 1k RGU ###############################################################################

,Tiquetes as(
  Select distinct Date_Trunc(Fecha_Apertura,Month) as TicketMonth,RIGHT(CONCAT('0000000000',CONTRATO),10) AS Contrato,count(distinct Tiquete) as TechCalls
  From `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220623_CR_TIQUETES_AVERIA_2021-01_A_2022-05_D`
  WHERE Clase is not null and Motivo is not null and Contrato is not null and estado <> "ANULADA"
  group by 1,2
)

,Tiquetes_fmc_Table as(
  Select f.*,TechCalls/ContractsFix as TechCall_Flag From FinalTablePlanAdj f left join Tiquetes
  ON RIGHT(CONCAT('0000000000',Fixed_Account),10)=Contrato and Month=safe_cast(TicketMonth as string)
)

############################################################### Care Calls ##########################################################################################

,Care_Calls as(
  Select distinct Date_Trunc(Fecha_Apertura,Month) as CallMonth,RIGHT(CONCAT('0000000000',CONTRATO),10) AS Contrato,count(distinct Tiquete_ID) as CareCalls
  From `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220623_CR_TIQUETES_SERVICIO_2021-01_A_2022-05_D`
  where CLASE IS NOT NULL AND MOTIVO IS NOT NULL AND CONTRATO IS NOT NULL
  and ESTADO <> "ANULADA" and TIPO <> "GESTION COBRO" --and MOTIVO <> "LLAMADA  CONSULTA DESINSTALACION" 
  --AND subarea<>"VENTA VIRTUAL" AND subarea<>"FECHA Y HORA DE VISITA"and subarea<>"FECHA Y HORA DE VISITA WEB"
  group by 1,2
)

,CareCalls_fmc_Table as (
  Select f.*,CareCalls/ContractsFix as CareCall_Flag From Tiquetes_fmc_Table f left join Care_Calls
  ON RIGHT(CONCAT('0000000000',Fixed_Account),10)=Contrato and Month=safe_cast(CallMonth as string)
)


########################################################## Billing Calls per Bill Variation ######################################################################

,AbsMRC AS (
SELECT *, abs(mrc_change) AS Abs_MRC_Change FROM CareCalls_fmc_Table

)
,BillVariations AS (
SELECT DISTINCT *,
CASE
WHEN Abs_MRC_Change>(TOTAL_B_MRC*(.05)) AND B_PLAN=E_PLAN AND no_plan_change_flag is not null THEN RIGHT(CONCAT('0000000000',Fixed_Account),10) ELSE NULL END AS BillVariation_flag
FROM AbsMRC
)

,BillVariation_MasterTable as(
  Select distinct f.*,BillVariation_Flag From CareCalls_fmc_Table f left join BillVariations b
  ON f.Fixed_Account=b.Fixed_Account and f.Month=b.Month
)

,BillVariation_Calls as(
  Select distinct Date_Trunc(Fecha_Apertura,Month) as CallMonth,RIGHT(CONCAT('0000000000',CONTRATO),10) AS Contrato,count(distinct Tiquete_ID) as BillingCalls
  From `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220623_CR_TIQUETES_SERVICIO_2021-01_A_2022-05_D`
  WHERE 
  CLASE IS NOT NULL AND MOTIVO IS NOT NULL AND CONTRATO IS NOT NULL
  AND ESTADO <> "ANULADA" AND TIPO <> "GESTION COBRO" AND MOTIVO = "CONSULTAS DE FACTURACION O COBRO" 
  group by 1,2
)

,BillVariationCalls_MasterTable as(
  Select f.*,BillingCalls From BillVariation_MasterTable f left join BillVariation_Calls
  ON Contrato=RIGHT(CONCAT('0000000000',Fixed_Account),10) and Month=safe_cast(CallMonth as string)
)


######################################################################## FTR Billing ################################################################################

,BillingCalls as (
  Select distinct * From `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220623_CR_TIQUETES_SERVICIO_2021-01_A_2022-05_D`
  WHERE CLASE IS NOT NULL AND MOTIVO IS NOT NULL AND CONTRATO IS NOT NULL
  AND ESTADO <> "ANULADA" AND TIPO <> "GESTION COBRO" AND MOTIVO = "CONSULTAS DE FACTURACION O COBRO"
)

,CallsWithoutSolution as(
  Select distinct a.Fecha_Apertura,a.Tiquete_ID, a.Contrato
  From BillingCalls a left join billingCalls b ON a.Contrato=b.Contrato
  Where date_diff(b.Fecha_Apertura,a.Fecha_Apertura,Day) between 0 and 14 and a.Tiquete_ID<>b.Tiquete_ID
  order by 3,1
)

,MultipleCallsFix as(
  Select distinct Fecha_Apertura,Max(Tiquete_ID) as Tiquete_ID,Contrato
  From CallsWithoutSolution
  group by 1,3
  order by 3,1
)

,AllBillingCallsJoin as(
  Select Distinct a.Fecha_Apertura,a.Contrato,a.Tiquete_ID,b.Tiquete_ID as RepeatedIssue
  From BillingCalls a left join MultipleCallsFix b
  ON a.Contrato=b.Contrato and a.Tiquete_ID=b.Tiquete_ID
  Where b.Tiquete_ID is null
)

,UniqueSuccesfulCalls as(
  Select Distinct Fecha_Apertura,Contrato,Max(Tiquete_ID) as Tiquete_ID
  From AllBillingCallsJoin
  group by 1,2
)

,SuccesfulCallsPerClient as(
  Select distinct date_trunc(Fecha_Apertura,Month) as TicketMonth,Contrato,Count(distinct Tiquete_ID) as ResolvedBilling
  From UniqueSuccesfulCalls
  group by 1,2
)

,SuccessfulCalls_MasterTable as(
  Select f.*,ResolvedBilling/ContractsFix as ResolvedBillingCalls
  From BillVariationCalls_MasterTable f left join SuccesfulCallsPerClient
  On Month=safe_cast(TicketMonth as string) and Fixed_Account=Contrato
)


############################################################## Grouped Table ##########################################################################################

Select Distinct Month,round(sum(B_NumRGUs/ContractsFix),0) as FixedRGUs,
round(sum(TechCall_Flag),0) as TechCalls,
round(sum(TechCall_Flag)*1000/sum(B_NumRGUs),0) as TechCallsPer1kRGU,
round(sum(CareCall_Flag),0) as CareCalls,
round(sum(CareCall_Flag)*1000/sum(B_NumRGUs),0) as CareCallsPer1kRGU,
Count(distinct BillVariation_Flag) as BillVariations,
sum(BillingCalls) as BillingCalls,
round(sum(BillingCalls)/Count(distinct BillVariation_Flag),3) as BillingCallsPerBillVariation,
round(sum(ResolvedBillingCalls),0) as FTR_Billing
From SuccessfulCalls_MasterTable
where month<>"2020-12-01" and Month <>"2022-06-01"
group by 1
order by 1
