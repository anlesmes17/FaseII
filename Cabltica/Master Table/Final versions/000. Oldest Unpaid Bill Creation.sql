WITH

FacturaCerrada as(
  SELECT DISTINCT FechaFactura,contrato,RIGHT(CONCAT('0000000000',factura) ,10) as factura,estado
  FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220628_Fact_Enca`
)


,PagoFactura as(
  SELECT DISTINCT RIGHT(CONCAT('0000000000',fact_aplica) ,10) as fact_aplica,FechaPago
  FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220628_Fact_Mov`
  group by 1,2
)

,TEST as( --Esta Consulta une una factura con su fecha de pago
  SELECT f.*,p.* FROM FacturaCerrada f LEFT JOIN PagoFactura p
  ON safe_cast(factura as string)=fact_aplica
)


,CRM as(
  SELECT * FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-06-08_CR_HISTORIC_CRM_ENE_2021_MAY_2022`
)

,Bill_and_Payment_Add AS( -- Esta consulta trae la fecha de la factur y la fecha de pago de la factura actual
  SELECT DISTINCT f.*,t.FechaFactura as Bill_Dt_M0,t.FechaPago as Bill_Payment_Date 
  FROM CRM f LEFT JOIN TEST t
  ON contrato=act_acct_cd AND Date_Trunc(FechaFactura,Month)=Date_Trunc(Fecha_Extraccion,Month)
)

,Last_Bill_Pym_Prel as( --Query para crear una tabla sólo con facturas únicas
  SELECT DISTINCT * except(estado) FROM TEST
)

,Last_Pym_CRM as( --Esta consulta ata el pago de la factura anterior al CRM
  SELECT DISTINCT f.*,FechaPago as Payment_Prev_Bill FROM Bill_and_Payment_Add f LEFT JOIN Last_Bill_Pym_Prel
  ON contrato=act_acct_cd AND Bill_Dt_M0=Date_Add(FechaFactura, INTERVAL 1 Month)

)

,TablaOldest as(
  SELECT DISTINCT a.* except(estado,fact_aplica,factura,FechaFactura), a.FechaFactura as FacturaActual,b.FechaFactura as Prev_Bill
  FROM TEST a LEFT JOIN TEST b 
  ON a.contrato=b.contrato AND a.FechaFactura=Date_add(b.FechaFactura, INTERVAL 1 MONTH)
  order by 1
)

,OldestPreliminary as(
  SELECT f.*,t.*, FROM Last_Pym_CRM f LEFT JOIN TablaOldest t
  ON act_acct_cd=contrato AND Date_Trunc(FECHA_EXTRACCION,Month)=FacturaActual
  where act_acct_cd=1126979
  order by fecha_extraccion
)

--,OldestStepOne as(
  SELECT f.*, CASE
  WHEN Fecha_Extraccion>=DATE(Bill_Payment_Date) THEN NULL
  WHEN DATE(Bill_Payment_Date)>Fecha_Extraccion AND Payment_Prev_Bill IS NULL AND Prev_Bill IS NULL THEN Bill_Dt_M0
  WHEN Fecha_Extraccion<DATE(Bill_Payment_Date) AND Fecha_Extraccion<DATE(Payment_Prev_Bill) THEN Prev_Bill
  WHEN FECHA_EXTRACCION>=DATE(Payment_Prev_Bill) AND Fecha_Extraccion<=DATE(Bill_Payment_Date) THEN Bill_Dt_M0
  WHEN Bill_Payment_Date IS NULL AND Payment_Prev_Bill IS NOT NUll THEN Prev_Bill
  ELSE Null END AS OLDEST_UNPAID
  FROM OldestPreliminary f
--)
