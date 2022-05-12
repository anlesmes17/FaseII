WITH

------------------------------------------------------- Saltos en mora luego de pago ------------------------------------------------------------------------------
SaltosMora AS (
  SELECT 	CST_CUST_NAME,act_acct_cd,FI_BILL_DT_M0,LST_PYM_DT,FI_OUTST_AGE,OLDEST_UNPAID_BILL_DT,FECHA_EXTRACCION FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
  WHERE act_acct_cd=1212882 and FECHA_EXTRACCION>="2021-02-01"
  ORDER BY FECHA_EXTRACCION
)

,SaltosMora2 AS (
  SELECT 	CST_CUST_NAME,act_acct_cd,FI_BILL_DT_M0,LST_PYM_DT,FI_OUTST_AGE,OLDEST_UNPAID_BILL_DT,FECHA_EXTRACCION FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
  WHERE act_acct_cd=1261572 and FECHA_EXTRACCION>="2021-08-01"
  ORDER BY FECHA_EXTRACCION
)

------------------------------------------------------ Oldest unpaid bill desaparecido sin pago ----------------------------------------------------------------------
,BillDesaparecido AS (
  SELECT 	CST_CUST_NAME,act_acct_cd,FI_BILL_DT_M0,LST_PYM_DT,FI_OUTST_AGE,OLDEST_UNPAID_BILL_DT,FECHA_EXTRACCION FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
  WHERE act_acct_cd=4121751 and FECHA_EXTRACCION>="2021-08-01"
  ORDER BY FECHA_EXTRACCION
)

------------------------------------------------------- Posible Mounting Bill ------------------------------------------------------------------------------------
,BillDesaparecido AS (
  SELECT 	CST_CUST_NAME,act_acct_cd,FI_BILL_DT_M0,LST_PYM_DT,FI_OUTST_AGE,OLDEST_UNPAID_BILL_DT,FECHA_EXTRACCION FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
  WHERE act_acct_cd=1218166 --and FECHA_EXTRACCION>="2021-02-01"
  ORDER BY FECHA_EXTRACCION
)

------------------------------------------------------ Reinicio de mora con pago -------------------------------------------
,Caso2 AS (
  SELECT 	CST_CUST_NAME,act_acct_cd,FI_BILL_DT_M0,LST_PYM_DT,FI_OUTST_AGE,OLDEST_UNPAID_BILL_DT,FECHA_EXTRACCION FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
  WHERE act_acct_cd=6013519 and FECHA_EXTRACCION>="2021-11-01"
  ORDER BY FECHA_EXTRACCION
)
---------------------------------------------- Problemas pesonas llegando a 60 y 90 -------------------------------------
WITH
MoraCheck AS(
SELECT DISTINCT FECHA_EXTRACCION,DATE_TRUNC(FECHA_EXTRACCION,MONTH) AS MONTH,ACT_ACCT_CD,OLDEST_UNPAID_BILL_DT,LST_PYM_DT,FI_OUTST_AGE
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`  
WHERE DATE_TRUNC(FECHA_EXTRACCION, MONTH) IN ('2021-05-01','2021-06-01','2021-07-01','2021-08-01','2021-09-01','2021-10-01') 
AND SAFE_CAST(FI_OUTST_AGE AS STRING) IN ('59','60','61','89','90','91')
)
SELECT MONTH, FI_OUTST_AGE,COUNT(DISTINCT ACT_ACCT_CD) 
FROM MoraCheck
group by 1,2
order by 1,2

