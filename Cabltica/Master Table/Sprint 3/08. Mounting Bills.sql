with 
initial_month as (
select 
    date_trunc(FECHA_EXTRACCION,MONTH) as Month,
    act_acct_cd, OLDEST_UNPAID_BILL_DT
from  `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
where FECHA_EXTRACCION=date_trunc(FECHA_EXTRACCION,MONTH)
)

,Day30 AS (
    select 
    date_trunc(FECHA_EXTRACCION,MONTH) as Month_day_30,
    act_acct_cd, OLDEST_UNPAID_BILL_DT, FI_BILL_DT_M0, TOT_BILL_AMT
from  `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
where FECHA_EXTRACCION=date_trunc(FECHA_EXTRACCION,MONTH)
)

,Day60 AS (
    select 
    date_trunc(FECHA_EXTRACCION,MONTH) as Month_day_60,
    act_acct_cd, OLDEST_UNPAID_BILL_DT, fi_bill_dt_m0
from  `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
where FECHA_EXTRACCION=date_trunc(FECHA_EXTRACCION,MONTH)
)

,MountingBills AS (
    SELECT c.Month_day_60, a.act_acct_cd,a.oldest_unpaid_bill_dt as unpaid_one, b.fi_bill_dt_m0 as Bill2,b.tot_bill_amt, c.oldest_unpaid_bill_dt as unpaid_three, c.fi_bill_dt_m0 as Bill3 FROM initial_month a left join Day30 b 
    ON a.act_acct_cd=b.act_acct_cd AND Month=date_trunc(date_add(b.Month_day_30, Interval 1 Month),Month) --AND b.fi_bill_dt_m0 is not null
    left join Day60 c ON a.act_acct_cd=c.act_acct_cd and a.Month=date_trunc(date_add(c.Month_day_60, Interval 2 Month),Month)
)

SELECT DISTINCT Month_day_60, count(distinct act_acct_cd)
FROM MountingBills
WHERE unpaid_one=unpaid_three AND Tot_bill_amt<>0 and tot_bill_amt is not null
GROUP BY 1
ORDER BY 1
