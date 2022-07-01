WITH

CHURNERSSO AS(
  SELECT DISTINCT NOMBRE_CONTRATO AS CONTRATOSO, DATE_TRUNC(FECHA_APERTURA,Month) as DeinstallationMonth,CASE
  WHEN submotivo="MOROSIDAD" THEN "Involuntary"
  WHEN submotivo<>"MOROSIDAD" THEN "Voluntary"
  END AS SUBMOTIVO
 FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220623_CR_ORDENES_SERVICIO_2021-01_A_2022-05_D`
 WHERE
  TIPO_ORDEN = "DESINSTALACION" 
  AND (ESTADO <> "CANCELADA" OR ESTADO <> "ANULADA")
 AND FECHA_APERTURA IS NOT NULL
)

,CRM as(
SELECT date_trunc(Max(Fecha_Extraccion),Month) as ChurnDate, act_acct_cd FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-06-08_CR_HISTORIC_CRM_ENE_2021_MAY_2022`
group by 2
)

,Trial as(
SELECT f.*,b.* FROM CRM f LEFT JOIN CHURNERSSO b
ON act_acct_cd=CONTRATOSO and deinstallationmonth=DeinstallationMonth
)

select distinct ChurnDate,SUBMOTIVO,count(distinct contratoso) from trial
where submotivo  IS NOT NULL
group by 1,2
order by 1
