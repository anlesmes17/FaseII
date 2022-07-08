WITH


##################################################################### Billing Tables #####################################################################
Bills as(--Database with all bills
  SELECT DISTINCT date(Fecha_Fact) as FechaFactura,safe_cast(contrato as int64) as contrato,RIGHT(CONCAT('0000000000',factura) ,10) as factura,
  FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.Fact_Enca_New`
)

,FirstBill as(
select distinct date_trunc(fechafactura,month) as mesfactura,contrato,
first_value(factura) over(partition by contrato,date_trunc(fechafactura,month) order by fechafactura asc) as PF
from Bills
)

,BillDate as(
select DISTINCT f.*,FechaFactura FROM FirstBill f LEFT JOIN Bills
ON PF=Factura
)

,PagoFactura as(--Database with payments of all bills
  SELECT DISTINCT RIGHT(CONCAT('0000000000',fact_aplica) ,10) as fact_aplica,Min(Fecha_mov) as FechaPago,
  FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.Fact_Mov_New`
  group by 1--,3,4,5
)

,CompleteBill as( --This query unifies bills with payments
  SELECT DISTINCT f.*,p.* FROM BillDate f LEFT JOIN PagoFactura p
  ON safe_cast(PF as string)=fact_aplica 
)

############################################################ Unión Billing CRM ########################################################################

,CRM as(
  SELECT DISTINCT * FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-06-08_CR_HISTORIC_CRM_ENE_2021_MAY_2022`
)

,Bill_and_Payment_Add AS( -- Brings current bill and payment of current bill
  SELECT DISTINCT f.*,t.FechaFactura as Bill_Dt_M0,t.FechaPago as Bill_Payment_Date 
  FROM CRM f LEFT JOIN CompleteBill t
  ON contrato=act_acct_cd AND Date_Trunc(FechaFactura,Month)=Date_Trunc(Fecha_Extraccion,Month)
)

,Last_Pym_CRM as( --Brings the previous bill and payment of previous bill
  SELECT DISTINCT f.*,t.FechaFactura as Prev_Bill,t.FechaPago as Payment_Prev_Bill, FROM Bill_and_Payment_Add f LEFT JOIN CompleteBill t
  ON contrato=act_acct_cd AND Bill_Dt_M0=Date_Add(FechaFactura, INTERVAL 1 Month)

)

,MinimumNoPayment as( --Brings the oldest bill that each client hasn't paid
SELECT DISTINCT contrato,Min(FechaFactura) as NoPaymentBill FROM CompleteBill
Where Fechapago is null
group by 1
)

,PerpetuityTablaOldest as(
  SELECT Distinct f.*,NoPaymentBill FROM Last_Pym_CRM f LEFT JOIN MinimumNoPayment b
  ON f.act_acct_cd=b.contrato
)

##################################################################### First Paid Bill ################################################################

,FirstAndLastBillsWithPayment as(--Brings oldest and newest bill a client has already paid
  select distinct
  first_value(mesfactura) over (partition by Contrato order by FechaPago asc) as MonthFirstPaidBill,
  first_value(FechaPago) over (partition by Contrato order by FechaPago asc) as PaymentFirstPaidBill,
  first_value(mesfactura) over (partition by Contrato order by FechaPago desc) as MonthLastPaidBill,
  first_value(FechaPago) over (partition by Contrato order by FechaPago desc) as PaymentLastPaidBill,
  contrato
  From CompleteBill
  where fechapago is not null
)

,FistPaidBillIntegration as(
  select distinct f.*,MonthFirstPaidBill,PaymentFirstPaidBill,MonthLastPaidBill,PaymentLastPaidBill, From PerpetuityTablaOldest f left join FirstAndLastBillsWithPayment
  ON contrato=act_acct_cd
)
#################################################### Oldest Unpaid Bill Selection ############################################

,OldestStepOne as(--This query selects the correct oldest unpaid bill given the clients current status
  SELECT DISTINCT f.*, CASE
  WHEN Fecha_Extraccion<date(PaymentFirstPaidBill) and Fecha_Extraccion>=MonthFirstPaidBill THEN MonthFirstPaidBill
  WHEN Fecha_Extraccion>=DATE(Bill_Payment_Date) THEN NULL
  WHEN DATE(Bill_Payment_Date)>Fecha_Extraccion AND Payment_Prev_Bill IS NULL AND Prev_Bill IS NULL THEN Bill_Dt_M0
  WHEN Fecha_Extraccion<DATE(Bill_Payment_Date) AND Fecha_Extraccion<DATE(Payment_Prev_Bill) THEN Prev_Bill
  WHEN FECHA_EXTRACCION>=DATE(Payment_Prev_Bill) AND Fecha_Extraccion<=DATE(Bill_Payment_Date) THEN Bill_Dt_M0
  WHEN Bill_Payment_Date IS NULL AND Payment_Prev_Bill IS NOT NUll AND FECHA_EXTRACCION<DATE(Payment_Prev_Bill) THEN Prev_Bill
  WHEN Bill_DT_M0 IS NOT NULL THEN NoPaymentBill
  WHEN Fecha_Extraccion<date(PaymentLastPaidBill) and Fecha_Extraccion>=MonthLastPaidBill THEN MonthLastPaidBill
  ELSE Null END AS OLDEST_UNPAID_BILL_DT_NEW--This is the field we are creating, its final name should be "OLDEST_UNPAID_BILL_DT"
  FROM FistPaidBillIntegration f
)

--,FI_Outst_Age as(--This query calculates the outstanding days of every user
Select Distinct * except(Bill_Payment_Date,Payment_Prev_Bill,Prev_Bill,NoPaymentBill,MonthFirstPaidBill,PaymentFirstPaidBill,MonthLastPaidBill,PaymentLastPaidBill)
,date_diff(Fecha_Extraccion,OLDEST_UNPAID_BILL_DT_NEW,Day) as FI_OUTST_AGE
FROM OldestStepOne
--)

