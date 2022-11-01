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
limit 10
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

--,NeverPaid_MasterTable as(
    Select f.*,NeverPaid_Flag
    From SoftDx_MasterTable f left join MaxOutstAge b ON act_acct_cd=Fixed_Account and cast(b.Sales_Month as date)=Month
--)
