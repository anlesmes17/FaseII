wITH

Sprint3Table AS (
SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Sprint3_Table_DashboardInput_v2`
)

########################################### Involuntary Funnel #######################################################

----------------------------------- Billing Columns Fix --------------------------------------


---Trae la fecha de oldest unpaid bill preliminar usando la última fecha de pago
,InitialAdjustment AS (
SELECT*, DATE_TRUNC(DATE_ADD(LST_PYM_DT, INTERVAL 1 MONTH),MONTH) AS OLDEST_UNPAID_BILL_ADJ
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
)

---Ajuste de oldest unpaid bill para usuarios que están al dia
,SecondAdjustment AS (
  SELECT *, CASE
  WHEN DATE_TRUNC(LST_PYM_DT,MONTH)=DATE_TRUNC(FECHA_EXTRACCION,MONTH) THEN NULL
  ELSE OLDEST_UNPAID_BILL_ADJ END AS OLDEST_UNPAID_BILL_FINAL
  FROM InitialAdjustment
)

---Calculo del fi outst age nuevo
,BillingColumnsAdjusted AS (
  SELECT *, DATE_DIFF(FECHA_EXTRACCION, OLDEST_UNPAID_BILL_FINAL, DAY) AS FI_OUTST_AGE_FINAL 
  FROM SecondAdjustment
)

------------------------------------ Involuntary Funnel ----------------------------------------

---Usuarios con facturas asignadas en el mes sin moras altas
,BillingMonth AS (
  SELECT *, DATE_TRUNC(FECHA_EXTRACCION, MONTH) AS Month 
  FROM BillingColumnsAdjusted
  WHERE DATE_TRUNC(FI_BILL_DT_M0,MONTH)=DATE_TRUNC(FECHA_EXTRACCION,MONTH) AND (FI_OUTST_AGE_FINAL=0 OR FI_OUTST_AGE_FINAL IS NULL) 

)
---Usuarios que llegan a dia 1 de mora
,OverdueMonth AS (
  SELECT *, DATE_TRUNC(FECHA_EXTRACCION, MONTH) AS Month 
  FROM BillingColumnsAdjusted
  WHERE FI_OUTST_AGE_FINAL>=21
)
---Usuarios que llegan a SoftDx
,SoftDxMonth AS (
  SELECT *, DATE_TRUNC(FECHA_EXTRACCION, MONTH) AS Month 
  FROM BillingColumnsAdjusted
  WHERE FI_OUTST_AGE_FINAL>=27
)
---Usuarios que llegan a HardDx, usar interval 2 en otras operaciones
,HardDxMonth AS (
  SELECT *, DATE_TRUNC(DATE_SUB(FECHA_EXTRACCION, INTERVAL 3 MONTH), MONTH) AS Month FROM BillingColumnsAdjusted
  WHERE FI_OUTST_AGE_FINAL>=90
)
---Funnel atando a mes base
,Funnel AS (
  SELECT DISTINCT a.Month AS InitialMonth, a.Act_acct_cd AS BillingAccount,b.Act_acct_cd AS OutstandingAccount,c.Act_acct_cd AS SoftDxAccount,d.Act_acct_cd AS HardDxAccount,
  FROM BillingMonth a LEFT JOIN OverdueMonth b ON a.act_acct_cd=b.act_acct_cd and a.Month=b.Month
  LEFT JOIN SoftDxMonth c ON a.act_acct_cd=c.act_acct_cd and a.Month=c.Month LEFT JOIN HardDxMonth d 
  ON a.act_acct_cd=d.act_acct_cd and a.Month=d.Month
)

,OutstandingAndSoftDxMasterTable AS (
  SELECT a.*, OutstandingAccount,SoftDxAccount
  FROM Sprint3Table a LEFT JOIN Funnel b ON a.Fixed_Account=b.BillingAccount AND a.Month=safe_cast(InitialMonth as string)
)
,HardDxMasterTable AS (
  SELECT a.*,HardDxAccount
  FROM OutstandingAndSoftDxMasterTable a LEFT JOIN Funnel b ON a.Fixed_Account=b.BillingAccount AND a.Month=safe_cast(DATE_ADD(InitialMonth, INTERVAL 3 MONTH) as string)
)
select * from HardDxMasterTable
where E_FINALtechflag is null
/*
select distinct month, count(distinct OutstandingAccount),count(distinct SoftDxAccount),count(distinct HardDxAccount),
FROM HardDxMasterTable
group by 1
order by 1
*/
##################################################### CSV File #################################################
/*
select distinct Month, E_FinalTechFlag, E_FMC_Segment,E_FMCType,E_TenureFinalFlag, 
count(distinct fixed_account) as activebase,count(distinct OutstandingAccount) AS OutstandingAccount, 
count(distinct SoftDxAccount) AS SoftDxAccount,count(distinct HardDxAccount) AS HardDxAccount FROM HardDxMasterTable
Group by 1,2,3,4,5*/
