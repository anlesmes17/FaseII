select cst_cust_name,act_acct_cd,act_cust_typ,act_acct_stat,act_cust_strt_dt,act_acct_inst_dt,fi_bill_dt_m0,bill_dt_m0,lst_pymt_dt,oldest_unpaid_bill_dt,fi_outst_age,dt FROM "db-analytics-dev"."dna_fixed_cr"
Where act_acct_cd='1421759'
order by dt
