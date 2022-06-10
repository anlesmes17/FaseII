WITH 


#################################################################################################################

FMC_Table AS (
SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Sprint3_Table_DashboardInput_v2`
)

########################################### Involuntary Funnel #######################################################

----------------------------------- Billing Columns Fix --------------------------------------


---Trae la fecha de oldest unpaid bill preliminar usando la última fecha de pago
,InitialAdjustment AS (
SELECT*, DATE_TRUNC(DATE_ADD(LST_PYM_DT, INTERVAL 1 MONTH),MONTH) AS OLDEST_UNPAID_BILL_ADJ
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-06-08_CR_HISTORIC_CRM_ENE_2021_MAY_2022`
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


#################################################################################################################









,Installations AS (
    SELECT DATE_TRUNC(FECHA_INSTALACION,MONTH) AS InstallationMonth,act_acct_cd, INSTALLATION_DT,monthsale_Flag,act_first_bill
    

    FROM (
        SELECT ACT_ACCT_CD, MIN(safe_cast(ACT_ACCT_INST_DT as date)) AS FECHA_INSTALACION,DATE(MIN(ACT_ACCT_INST_DT)) AS INSTALLATION_DT,
        CASE WHEN ACT_ACCT_CD IS NOT NULL THEN ACT_ACCT_CD ELSE NULL END AS monthsale_Flag,
        concat(act_acct_cd, first_value(OLDEST_UNPAID_BILL_FINAL) over(partition by act_acct_cd order by FECHA_EXTRACCION)) as act_first_bill
        FROM BillingColumnsAdjusted
        GROUP BY 1,OLDEST_UNPAID_BILL_FINAL,FECHA_EXTRACCION
    )
    GROUP BY 1,2,3,4,5
)
,all_overdue_days as (
select 
   date_trunc(cast (ACT_ACCT_INST_DT as date),Month) as Sales_Month,
   date_trunc(cast(act_acct_inst_dt as date),Month) as Inst_Month,
    act_acct_cd, OLDEST_UNPAID_BILL_FINAL, act_acct_inst_dt,
    case when fi_outst_age is null then -1 else cast(fi_outst_age as int) end as fi_outst_age,lst_pym_dt,
from 
    ( select * from  BillingColumnsAdjusted)
where concat(safe_cast(act_acct_cd as string),safe_cast(OLDEST_UNPAID_BILL_FINAL as string)) in (select (safe_cast(act_first_bill as string)) from Installations)
)

,account_summary AS (
    SELECT 
        Sales_Month,
        Inst_Month,
        act_acct_cd,
        min(act_acct_inst_dt) as inst_date,
        /*max(CRM) as CRM,*/
        MAX(fi_outst_age) AS MAX_OUTS_AGE,
        MIN(OLDEST_UNPAID_BILL_FINAL) as MIN_OLDEST_UNPAID_BILL_DT,
        MAX(OLDEST_UNPAID_BILL_FINAL) as MAX_OLDEST_UNPAID_BILL_DT,
        max(cast (lst_pym_dt as date) ) AS LAST_LST_PYMT_DT,
        CASE WHEN (MAX(fi_outst_age) >=21 ) THEN 1 ELSE 0 END AS SOFT_DX,
        CASE WHEN (MAX(fi_outst_age) >=90 and  max(cast (lst_pym_dt as date)) is null ) THEN 1 else 0 END AS CHURNERS_NEVER_PAID
    from all_overdue_days
    GROUP BY Sales_Month, Inst_Month, act_acct_cd
)

select distinct date_trunc(sales_month,month), count (distinct act_acct_cd) from account_summary where soft_dx=1
and sales_month>="2021-01-01"
group by 1
order by 1

/*
--,SoftDx_NeverPaid_Flag AS(
    select f.*, a.act_acct_cd, Soft_Dx as SoftDx_Flag, Churners_Never_Paid as NeverPaid_Flag,
    CASE WHEN a.act_acct_cd is not null then 1 else 0 end as monthsale_flag
    FROM FMC_Table f left join account_summary a
    ON f.fixed_account = a.act_acct_cd and f.month = a.inst_month
 
--)*/
