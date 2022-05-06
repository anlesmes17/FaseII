--------------------------------------------------------------------------------
 --------------- LIBERATE ONLY FOR GCP---------------------------------------
 --------------------------------------------------------------------------------
with table_account_liberate as(
    select *, REGEXP_REPLACE(cast(ACCOUNT_ID as varchar),'[^0-9 ]','') AS STANDARDACC,
    SPLIT_PART (CLEAR_TIME, '/', 1) as Clear_Day, 
    SPLIT_PART ( CLEAR_TIME, '/',2) as Clear_Month,
    SPLIT_PART ( SPLIT_PART (CLEAR_TIME, '/', 3),' ',1) as Clear_Year,
    SPLIT_PART ( CLEAR_TIME, ' ',2) as Clear_Time,
    SPLIT_PART (RECEIVED_TIME, '/', 1) as Received_Day, 
    SPLIT_PART (RECEIVED_TIME, '/',2) as Received_Month,
    SPLIT_PART ( SPLIT_PART (RECEIVED_TIME, '/', 3),' ',1) as Received_Year,
    SPLIT_PART ( RECEIVED_TIME, ' ',2) as Received_Time
    FROM "lla_cco_int_ext"."cwc_fix_ext_techtickets"
    WHERE lower(CUST_TYPE) like '%residential%' and FOUND_NAME <> 'NETWORK MIGRATION'
 -- and DATE_PARSE(replace(substring(RECEIVED_TIME,1,STRPOS(RECEIVED_TIME,' ')-1),'/','-'),'%m-%d-%Y') between date('2022-02-01') and date('2022-02-28')
    )

,Date_Replacements AS(
SELECT *, 
REGEXP_REPLACE(cast(Clear_Day as varchar),'[^0-9 ]','') as Clear_Day,
REGEXP_REPLACE(cast(Clear_Month as varchar),'[^0-9 ]','') as Clear_Month,
REGEXP_REPLACE(cast(Clear_Year as varchar),'[^0-9 ]','') as Clear_Year,
REGEXP_REPLACE(cast(Received_Day as varchar),'[^0-9 ]','') as Received_Day_Adj,
REGEXP_REPLACE(cast(Received_Month as varchar),'[^0-9 ]','') as Received_Month_Adj,
REGEXP_REPLACE(cast(Received_Year as varchar),'[^0-9 ]','') as Received_Year_Adj
FROM table_account_liberate
)

,Def_DateParts as(
SELECT *,
CASE WHEN cast(Received_Month_Adj as int) > 12 AND cast(Received_Day_Adj as int) <= 12 THEN CAST(Received_Day_Adj AS INT)
WHEN cast( Received_Month_Adj as int) < 12 THEN CAST(Received_Month_Adj AS INT) END AS Month_def
,CASE WHEN cast(Received_Month_Adj  as int) > 12 AND cast(Received_Day_Adj as int) <= 12 THEN CAST(Received_Month_Adj AS INT)
WHEN cast(Received_Month_Adj  as int) < 12 and cast(Received_Day_Adj as bigint) <= 31 THEN CAST(Received_Day_Adj AS INT) END AS Day_def,
CAST(Received_Year_Adj AS INT) AS Year_def
FROM Date_Replacements
WHERE Received_day_Adj <> '' AND Received_Month_adj <> '' and Received_Year_adj <> ''
)

,DateAdjustments as(
select *, 
CASE
WHEN LENGTH(cast(Day_def as varchar))=2 THEN cast(Day_def as varchar)
WHEN LENGTH(cast(Day_def as varchar))=1 THEN CONCAT('0',coalesce(cast(Day_def as varchar),'')) END as Day_Fin,
CASE
WHEN LENGTH(cast(Month_def as varchar))=2 THEN cast(Month_def as varchar)
WHEN LENGTH(cast(Month_def as varchar))=1 THEN CONCAT('0',coalesce(cast(Month_def as varchar),'')) END as Month_Fin,
CASE
WHEN 
from Def_DateParts
)
,DateConcat AS (

)

--, FinalDate as(
    select *,
    CONCAT(cast(Day_def as varchar), '-', cast(Month_def as varchar), '-', cast(Year_Def as varchar)) as Final_Received_dt
    FROM Def_dateParts
)


,cast_variables AS (
    SELECT TICKET_NUMBER as TICKET_NUMBER, ACCOUNT_ID AS OLDACCOUNT_ID,cast(STANDARDACC as BIGINT) AS ACCOUNT_ID,
    CLEARING_COMMENT, REPORT_TITLE,
    date_parse(Final_received_dt,'%d-%m-%Y') as Received_Time,
    CASE WHEN LENGTH(STANDARDACC) = 8 THEN 'CERILLION' ELSE 'LIBERATE' END AS CRM 
    FROM FinalDate
    WHERE LENGTH(STANDARDACC) IN (8,12) AND STATUS = 'CLOSED' 
    
),

 grouped_table_liberate AS(
    select TICKET_NUMBER, cast(ACCOUNT_ID as varchar) as ACCOUNT_ID, min(RECEIVED_TIME) as RECEIVED_TIME
    FROM cast_variables
    where extract(MONTH from RECEIVED_TIME)=2 AND extract(YEAR from RECEIVED_TIME) =2022
    GROUP BY TICKET_NUMBER, ACCOUNT_ID
    ),

dna as (
    select act_acct_cd, min(date(act_cust_strt_dt)) as act_cust_strt_dt
    from 
    (
    select * from "db-analytics-prod"."tbl_fixed_cwc"
    )
    where org_cntry='Jamaica' 
    AND ACT_CUST_TYP_NM IN ('Browse & Talk HFONE', 'Residence', 'Standard') 
    AND ACT_ACCT_STAT IN ('B','D','P','SN','SR','T','W') AND  date(act_cust_strt_dt) between 
    date('2021-12-01') AND date('2022-02-28')
    GROUP BY act_acct_cd
),

joint_tables as (
select a.* ,b.act_cust_strt_dt
from grouped_table_liberate a
left join dna b
on a.ACCOUNT_ID =b.act_acct_cd
),

accounts_with_early_tickets as (
select ACCOUNT_ID, max(case when date_diff('Week',act_cust_strt_dt,RECEIVED_TIME)<=7 then 1 else 0 end ) as early_tickets
from joint_tables
GROUP by ACCOUNT_ID
)

select sum(early_tickets)
from accounts_with_early_tickets;
