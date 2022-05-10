with 

DateCreation as(
    select *,REGEXP_REPLACE(cast(ACCOUNT_ID as varchar),'[^0-9 ]','') AS STANDARDACC,
    SPLIT_PART (RECEIVED_TIME, ' ', 1) as TicketDate
    from "cwc_fix_ext_techtickets"
)
    
,FinalDate as (
    select*,date_parse(TicketDate,'%m/%d/%Y') as FinalDate
    From DateCreation
)


,cast_variables AS (
    SELECT TICKET_NUMBER as TICKET_NUMBER, ACCOUNT_ID AS OLDACCOUNT_ID, ACCOUNT_ID,cast(FinalDate as date) as FinalDate,
    CLEARING_COMMENT, REPORT_TITLE,
    CASE WHEN LENGTH(STANDARDACC) = 8 THEN 'CERILLION' ELSE 'LIBERATE' END AS CRM 
    FROM FinalDate
    WHERE LENGTH(STANDARDACC) IN (8,12) AND STATUS = 'CLOSED' 
    
)

 ,grouped_table_liberate AS(
    select TICKET_NUMBER, cast(ACCOUNT_ID as varchar) as ACCOUNT_ID, min(FinalDate) as FinalDate
    FROM cast_variables
    where FinalDate>date('2021-01-01')
    GROUP BY TICKET_NUMBER, ACCOUNT_ID
    )

,dna as (
    select act_acct_cd, min(date(act_cust_strt_dt)) as act_cust_strt_dt
    from 
    (
    select * from "db-analytics-prod"."tbl_fixed_cwc"
    )
    where org_cntry='Jamaica' 
    AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
    AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W') 
    GROUP BY act_acct_cd
),

joint_tables as (
select a.* ,b.act_cust_strt_dt
from grouped_table_liberate a
left join dna b
on a.ACCOUNT_ID =b.act_acct_cd
)

,accounts_Liberate as (
select ACCOUNT_ID, max(case when date_diff('Week',act_cust_strt_dt,FinalDate)<=7 then 1 else 0 end ) as early_tickets, act_cust_strt_dt as Sales_Month
from joint_tables
GROUP by ACCOUNT_ID,act_cust_strt_dt
order by 2 desc
)



############################## Cerillion #################################

,initial_table_cer as (
SELECT date(date_trunc('MONTH', interaction_start_time)) as Ticket_Month, interaction_id, account_id_2, min(date(interaction_start_time)) as interaction_start_time
FROM (select *, REGEXP_REPLACE(account_id,'[^0-9 ]','') as account_id_2
from "db-analytics-dev"."interactions_cwc"
where lower(org_cntry) like '%jam%') --AND date(interaction_start_time)  between
--date('2022-02-01') AND date('2022-02-28')) 
GROUP BY 1,Interaction_id, account_id_2
)

,installations as (
select 
    date_trunc('Month', min(date(act_cust_strt_dt))) as Sales_Month,
    act_acct_cd, min(date(act_cust_strt_dt)) as act_cust_strt_dt
from   "db-analytics-prod"."tbl_fixed_cwc"
where org_cntry = 'Jamaica' AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W') and date(act_cust_strt_dt) between date_trunc('Month',cast (act_cust_strt_dt as date)) and date_add('DAY',90, date_trunc('Month',cast (act_cust_strt_dt as date)))

--cast('2021-12-01' as date) and cast('2022-02-28' as date)
GROUP BY act_acct_cd
),

joint_bases as (
select initial_table_cer.*, installations.sales_month, installations.act_cust_strt_dt
from initial_table_cer 
left join installations 
on initial_table_cer.account_id_2 = installations.act_acct_cd
)

,accounts_Cerillion as (
select account_id_2 as ACCOUNT_ID, max(case when date_diff('week',act_cust_strt_dt,interaction_start_time)<=7 then 1 else 0 end ) as early_tickets, Sales_Month
from joint_bases
GROUP by Sales_Month, account_id_2
)

--,TwoSystems as (
select distinct *
from (select * from accounts_Liberate
UNION ALL
select * from accounts_Cerillion
)
--)

