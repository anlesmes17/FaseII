WITH


##################################################################### Tablas Billing ############################################################################
Facturas as(
  SELECT DISTINCT FechaFactura,contrato,RIGHT(CONCAT('0000000000',factura) ,10) as factura
  FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220628_Fact_Enca`
)

,PrimeraFactura as(
select distinct date_trunc(fechafactura,month) as mesfactura,contrato,first_value(factura) over(partition by contrato,date_trunc(fechafactura,month) order by fechafactura asc) as PF
from Facturas
)

,FechaFactura as(
select DISTINCT f.*,FechaFactura FROM PrimeraFactura f LEFT JOIN Facturas
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
order by Fecha_Extraccion
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
  Order by 1 desc
)

##################################################################### No Bill Emission ###############################################################################################
/*
,AllBillsCRM as( --Consulta que trae todas las facturas de cada cliente y las ata al CRM
  SELECT f.act_acct_cd, f.Fecha_Extraccion,b.FechaFactura as BillNoEmission, FechaPago as PaymentNoBill
  FROM CRM f LEFT JOIN TEST b
  ON contrato=act_acct_cd
)

,OldestNoBillEmission as( --Consulta que crea una columna adicional para asignar Oldest a clientes que no le emitieron factura ese mes
Select *, CASE
WHEN Date_trunc(Fecha_Extraccion,Month)<>BillNoEmission AND Fecha_Extraccion>BillNoEmission AND Fecha_Extraccion<DATE(PaymentNoBill) THEN BillNoEmission
ELSE NULL END AS OldestNoBill
FROM AllBillsCRM
order by fecha_extraccion
)

,AllBillsNoPayment as(
Select * From OldestNoBillEmission WHERE OldestNoBill IS NOT NULL
)

,BillsNoPaymentCRM as(
  SELECT DISTINCT f.*,Min(OldestNoBill) as OldestNoBill,PaymentNoBill  FROM PerpetuityTablaOldest f LEFT JOIN AllBillsNoPayment b
  ON f.act_acct_cd=b.act_acct_cd AND f.Fecha_Extraccion=b.Fecha_extraccion
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,44
)
*/

,OldestStepOne as(
  SELECT DISTINCT f.*, CASE
  WHEN Fecha_Extraccion>=DATE(Bill_Payment_Date) THEN NULL
  WHEN DATE(Bill_Payment_Date)>Fecha_Extraccion AND Payment_Prev_Bill IS NULL AND Prev_Bill IS NULL THEN Bill_Dt_M0
  WHEN Fecha_Extraccion<DATE(Bill_Payment_Date) AND Fecha_Extraccion<DATE(Payment_Prev_Bill) THEN Prev_Bill
  WHEN FECHA_EXTRACCION>=DATE(Payment_Prev_Bill) AND Fecha_Extraccion<=DATE(Bill_Payment_Date) THEN Bill_Dt_M0
  WHEN Bill_Payment_Date IS NULL AND Payment_Prev_Bill IS NOT NUll AND FECHA_EXTRACCION<DATE(Payment_Prev_Bill) THEN Prev_Bill
  WHEN Bill_DT_M0 IS NOT NULL THEN OldestPerpetuity

  --WHEN FECHA_EXTRACCION>DATE(PaymentNoBill) THEN NULL
  --WHEN OldestNoBill IS NOT NULL AND Bill_DT_M0 IS NULL AND Fecha_Extraccion<DATE(PaymentNoBill) THEN OldestNoBill
  ELSE Null END AS OLDEST_UNPAID
  FROM PerpetuityTablaOldest f
order by fecha_extraccion
)

Select Distinct * except(Bill_Payment_Date,Payment_Prev_Bill,Prev_Bill,OldestPerpetuity/*,OldestNoBill*/)
FROM OldestStepOne
where act_acct_cd=761455
order by fecha_extraccion























/*
SELECT DISTINCT FECHA_EXTRACCION, ACT_ACCT_CD,count(act_acct_cd) FROM OldestStepOne
group by 1,2
order by 3 desc
*/
