WITH

FirstBill as(
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

--,MaxOutstAge as(
    Select Distinct Sales_Month,act_acct_cd,Max(FI_OUTST_AGE_NEW) as Outstanding_Days,
    Case when Max(FI_OUTST_AGE_NEW)>=26 Then act_acct_cd ELSE NULL END AS SoftDx_Flag,
    Case when Max(FI_OUTST_AGE_NEW)>=90 Then act_acct_cd ELSE NULL END AS NeverPaid_Flag,
    From JoinFirstBill
    group by 1,2
    order by 2,3
--)

