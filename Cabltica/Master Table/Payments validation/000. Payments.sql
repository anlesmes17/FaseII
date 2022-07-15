WITH

Payments as(
  select  distinct * From `dev-fortress-335113.cabletica_ontological_prod_final.payment`
  where date_trunc(CLEARING_DATE,Month)="2022-05-01" and INVOICE_ID<>"-1" --and invoice_id="0000621239"
  --group by 1,2
)

,unique_payments as(
  select distinct invoice_id,sum(payment_amt_local) as payment_total From Payments
  group by 1
)

##################################################################### Billing Tables #####################################################################
,Bills as(--Database with all bills
  SELECT DISTINCT date(Fecha_Fact) as FechaFactura,safe_cast(contrato as int64) as contrato,RIGHT(CONCAT('0000000000',factura) ,10) as factura,
  FROM `dev-fortress-335113.Payment_Fact_Table.Fact_Enca` 
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
  SELECT DISTINCT RIGHT(CONCAT('0000000000',fact_aplica) ,10) as fact_aplica,Min(Fecha_mov) as FechaPago,round(sum(Monto_fact),0) as Monto_Pago
  FROM `dev-fortress-335113.Payment_Fact_Table.Fact_Mov`
  group by 1--,3,4,5
)

,CompleteBill as( --This query unifies bills with payments
  SELECT DISTINCT f.*,p.* FROM BillDate f LEFT JOIN PagoFactura p
  ON safe_cast(PF as string)=fact_aplica 
  where MesFactura="2022-05-01"
)

,Join_Bases as(
  select a.*,b.* From unique_payments a inner join completebill b
  on pf=invoice_id
)

--,FinalTable as (
  select *,payment_total-Monto_Pago as difference From join_bases
  where fact_aplica is not null
--)
