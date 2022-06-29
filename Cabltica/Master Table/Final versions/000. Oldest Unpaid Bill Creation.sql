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

,TEST as(
  SELECT f.*,p.* FROM FacturaCerrada f LEFT JOIN PagoFactura p
  ON safe_cast(factura as string)=fact_aplica
)

,CRM as(
  SELECT * FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-06-08_CR_HISTORIC_CRM_ENE_2021_MAY_2022`
)

,Bill_and_Payment_Add AS(
  SELECT DISTINCT f.*,t.FechaFactura as Bill_Dt_M0,t.FechaPago as Bill_Payment_Date 
  FROM CRM f LEFT JOIN TEST t
  ON contrato=act_acct_cd AND Date_Trunc(FechaFactura,Month)=Date_Trunc(Fecha_Extraccion,Month)
)

,Last_Bill_Pym_Prel as(
  SELECT DISTINCT * except(estado) FROM TEST
)

--,Last_Pym_CRM as(
  SELECT DISTINCT f.*,FechaPago as Payment_Prev_Bill FROM Bill_and_Payment_Add f LEFT JOIN Last_Bill_Pym_Prel
  ON contrato=act_acct_cd AND Bill_Dt_M0=Date_Add(FechaFactura, INTERVAL 1 Month)
  where act_acct_cd=1126979
  order by fecha_extraccion
--)
