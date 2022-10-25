with 

parameters as (
select
-----------------------------------------------------
-- Change Date in this line to define query period 
(DATE('2022-09-01') + interval '1' MONTH - interval '1' DAY - interval '2' MONTH) as start_date,
(DATE('2022-09-01') + interval '1' MONTH - interval '1' DAY) as end_date,
90 as max_overdue_active_base
)
-----------------------------------------------------

Select * From "db-analytics-dev"."dna_fixed_cr"
WHERE date(dt) BETWEEN (select start_date from parameters) and (select end_date from parameters)

