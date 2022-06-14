WITH

MasterTable AS(
Select * FROM "lla_cco_int_ana_prod"."cwc_fmc_churn_prod" 
Where date(Month)=date(dt)
)
--------------------------------------------------------------------- FMC Calculation ----------------------------------------------------------------------------

,FMC_Feb AS(
Select * from "lla_cco_int_ext"."cwc_con_ext_fmc"
WHERE dt='2022-02-01'and business_unit = 'Jamaica' 
and account_type = 'Residential' AND service_type <> 'Service Not Found' AND service_type IS NOT NULL
)

,FMC_Apr AS(
Select *, date(distinct date_parse(fmc_start_date,'%m/%d/%Y')) as DatePrel from "lla_cco_int_ext"."cwc_con_ext_fmc"
WHERE dt='2022-04-01' and fmc_start_date<>'nan'
and business_unit = 'Jamaica' 
and account_type = 'Residential' AND service_type <> 'Service Not Found' AND service_type IS NOT NULL
)

,FMC_Mar_prel AS(
Select Distinct a.Account_no as FMC_Account,b.fmc_start_date, CASE
WHEN a.Account_no IS NOT NULL THEN '2022-03-01' ELSE NULL END AS FMC_Month
FROM FMC_Feb a LEFT JOIN FMC_Apr b ON Date(a.dt)=Date_Add('Month',-2,Date(b.dt)) AND a.Account_no=b.Account_no
)

,FMC_MarMasterTable AS(
Select f.*,FMC_Account as FMC_join FROM MasterTable f LEFT JOIN FMC_Mar_prel ON cast(Month as varchar)=cast(FMC_Month as varchar) AND cast(FMC_Account as varchar)=cast(Final_Account as varchar)
)

,FMC_Missing AS (
Select * FROM FMC_Apr
where date_trunc('Month',date(DatePrel))=date('0022-03-01')
)

,FMC_MissingMasterTable AS(
Select f.*,b.account_no as FMC_Missing
FROM FMC_MarMasterTable f LEFT JOIN FMC_Missing b ON cast(Month as varchar)=b.dt 
AND cast(b.Account_no as varchar)=cast(Final_Account as varchar)
)

,FMC_MarFinal AS(
Select *, CASE
WHEN FMC_Join IS NOT NULL THEN FMC_Join
WHEN FMC_Missing IS NOT NULL THEN FMC_Missing
ELSE NULL END AS MarFMC
FROM FMC_MissingMasterTable
)

,SegmentAndType AS(
Select *, 
CASE
WHEN MarFMC IS NOT NULL AND b_mixcode_adj =
)
