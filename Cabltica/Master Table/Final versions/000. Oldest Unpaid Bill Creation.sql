WITH


##################################################################### Tablas Billing ############################################################################
Bills as(
  SELECT DISTINCT FechaFactura,contrato,RIGHT(CONCAT('0000000000',factura) ,10) as factura
  FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220628_Fact_Enca`
)

,PrimeraFactura as(
select distinct date_trunc(fechafactura,month) as mesfactura,contrato,first_value(factura) over(partition by contrato,date_trunc(fechafactura,month) order by fechafactura asc) as PF
from Bills
)

,FechaFactura as(
select DISTINCT f.*,FechaFactura FROM PrimeraFactura f LEFT JOIN Bills
ON PF=Factura
)

,PagoFactura as(
  SELECT DISTINCT RIGHT(CONCAT('0000000000',fact_aplica) ,10) as fact_aplica,Min(FechaPago) as FechaPago
  FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220628_Fact_Mov`
  group by 1
)

,TEST as( --Esta Consulta une una factura con su fecha de pago
  SELECT DISTINCT f.*,p.* FROM FechaFactura f LEFT JOIN PagoFactura p
  ON safe_cast(PF as string)=fact_aplica
)

################################################################# Unión Billing CRM ###############################################################################

,CRM as(
  SELECT DISTINCT * FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-06-08_CR_HISTORIC_CRM_ENE_2021_MAY_2022`
)

,Bill_and_Payment_Add AS( -- Esta consulta trae la fecha de la factura y la fecha de pago de la factura actual
  SELECT DISTINCT f.*,t.FechaFactura as Bill_Dt_M0,t.FechaPago as Bill_Payment_Date 
  FROM CRM f LEFT JOIN TEST t
  ON contrato=act_acct_cd AND Date_Trunc(FechaFactura,Month)=Date_Trunc(Fecha_Extraccion,Month)
)

,Last_Pym_CRM as( --Esta consulta ata el pago de la factura anterior al CRM
  SELECT DISTINCT f.*,t.FechaFactura as Prev_Bill,t.FechaPago as Payment_Prev_Bill, FROM Bill_and_Payment_Add f LEFT JOIN TEST t
  ON contrato=act_acct_cd AND Bill_Dt_M0=Date_Add(FechaFactura, INTERVAL 1 Month)

)

,OldestPerpetuity as( --Consulta que trae la mímima factura de cada cliente que no ha sido pagada
SELECT DISTINCT contrato,Min(FechaFactura) as OldestPerpetuity FROM TEST
Where Fechapago is null
group by 1
)

,PerpetuityTablaOldest as(--Tabla que muestra mes actual, fecha de pago de aftura del es, factura anterior, y mímima factura que no presenta pago
  SELECT Distinct f.*,OldestPerpetuity FROM Last_Pym_CRM f LEFT JOIN OldestPerpetuity b
  ON f.act_acct_cd=b.contrato
)

##################################################################### No Bill Emission ###############################################################################################

,FirstAndLastBillsWithPayment as(
  select distinct
  first_value(mesfactura) over (partition by Contrato order by FechaPago asc) as MonthFirstPaidBill,
  first_value(FechaPago) over (partition by Contrato order by FechaPago asc) as PaymentFirstPaidBill,
  first_value(mesfactura) over (partition by Contrato order by FechaPago desc) as MonthLastPaidBill,
  first_value(FechaPago) over (partition by Contrato order by FechaPago desc) as PaymentLastPaidBill,
  contrato
  From TEST
  where fechapago is not null
)

,FistPaidBillIntegration as(
  select f.*,MonthFirstPaidBill,PaymentFirstPaidBill,MonthLastPaidBill,PaymentLastPaidBill, From PerpetuityTablaOldest f left join FirstAndLastBillsWithPayment
  ON contrato=act_acct_cd
)
,OldestStepOne as(
  SELECT DISTINCT f.*, CASE
  WHEN Fecha_Extraccion<date(PaymentFirstPaidBill) and Fecha_Extraccion>=MonthFirstPaidBill THEN MonthFirstPaidBill
  WHEN Fecha_Extraccion>=DATE(Bill_Payment_Date) THEN NULL
  WHEN DATE(Bill_Payment_Date)>Fecha_Extraccion AND Payment_Prev_Bill IS NULL AND Prev_Bill IS NULL THEN Bill_Dt_M0
  WHEN Fecha_Extraccion<DATE(Bill_Payment_Date) AND Fecha_Extraccion<DATE(Payment_Prev_Bill) THEN Prev_Bill
  WHEN FECHA_EXTRACCION>=DATE(Payment_Prev_Bill) AND Fecha_Extraccion<=DATE(Bill_Payment_Date) THEN Bill_Dt_M0
  WHEN Bill_Payment_Date IS NULL AND Payment_Prev_Bill IS NOT NUll AND FECHA_EXTRACCION<DATE(Payment_Prev_Bill) THEN Prev_Bill
  WHEN Bill_DT_M0 IS NOT NULL THEN OldestPerpetuity
  WHEN Fecha_Extraccion<date(PaymentLastPaidBill) and Fecha_Extraccion>=MonthLastPaidBill THEN MonthLastPaidBill
  ELSE Null END AS OLDEST_UNPAID
  FROM FistPaidBillIntegration f
)

--,FI_Outst_Age as(
Select Distinct * except(Bill_Payment_Date,Payment_Prev_Bill,Prev_Bill,OldestPerpetuity,MonthFirstPaidBill,PaymentFirstPaidBill,MonthLastPaidBill,PaymentLastPaidBill)
,date_diff(Fecha_Extraccion,OLDEST_UNPAID,Day) as Outstanding_Days
FROM OldestStepOne
--)
