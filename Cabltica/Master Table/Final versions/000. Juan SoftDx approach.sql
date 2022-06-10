-- STRATIGHT TO SOFT DX
        -----------------------------------------
        WITH 
        parameters as (
        select date('2022-03-01') as month_analysis,
        21 as non_pay_threshold
        )
        
        ,monthly_inst_accounts as (
        SELECT 
            act_acct_cd
        FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-06-08_CR_HISTORIC_CRM_ENE_2021_MAY_2022`
        WHERE 
            DATE_TRUNC(FECHA_EXTRACCION,Month) = (select month_analysis from parameters)
            and DATE_TRUNC(FECHA_EXTRACCION,Month) = (select month_analysis from parameters)
        )

        ,first_bill as (
        SELECT act_acct_cd, concat(max(act_acct_cd),'-',min(first_oldest_unpaid_bill_dt)) as act_first_bill
        FROM
            (select act_acct_cd,
            FIRST_VALUE(date(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY FECHA_EXTRACCION) AS first_inst_dt, 
            FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY FECHA_EXTRACCION) AS first_oldest_unpaid_bill_dt
            FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-06-08_CR_HISTORIC_CRM_ENE_2021_MAY_2022`
            WHERE
            act_acct_cd in (select act_acct_cd from monthly_inst_accounts)
            AND FECHA_EXTRACCION between ((select month_analysis from parameters ) - interval '6' month) and ((select month_analysis from parameters ) + interval '2' month))
        where DATE_TRUNC(first_inst_dt,Month) = (select month_analysis from parameters)
        group by act_acct_cd
        ),

        max_overdue_first_bill as (
        select act_acct_cd, 
        min(first_oldest_unpaid_bill_dt) as first_oldest_unpaid_bill_dt,
        min(first_inst_dt) as first_inst_dt, min(first_act_cust_strt_dt) as first_act_cust_strt_dt,
        concat(max(act_acct_cd),'-',min(first_oldest_unpaid_bill_dt))  as act_first_bill,
        max(fi_outst_age) as max_fi_outst_age, 
        max(fecha_extraccion) as max_dt,
        case when max(fi_outst_age)>=(select non_pay_threshold from parameters ) then 1 else 0 end as soft_dx_flg
        FROM
            (select act_acct_cd,
            FIRST_VALUE(oldest_unpaid_bill_dt) OVER (PARTITION BY act_acct_cd ORDER BY FECHA_EXTRACCION) AS first_oldest_unpaid_bill_dt,
            FIRST_VALUE(date(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY FECHA_EXTRACCION) AS first_inst_dt, 
            FIRST_VALUE(date(act_acct_inst_dt)) OVER (PARTITION BY act_acct_cd ORDER BY FECHA_EXTRACCION) AS first_act_cust_strt_dt,
            fi_outst_age, FECHA_EXTRACCION, pd_mix_cd,
            FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-06-08_CR_HISTORIC_CRM_ENE_2021_MAY_2022`
            WHERE 
            concat(act_acct_cd,'-',oldest_unpaid_bill_dt) in (select act_first_bill from first_bill)
            AND FECHA_EXTRACCION between (select month_analysis from parameters ) and ((select month_analysis from parameters ) + interval '5' month)
            )
        group by act_acct_cd

        )

        select *, (select month_analysis from parameters) as month_analysis,
        first_oldest_unpaid_bill_dt + interval '21' day as threshold_pay_date,
        case when (first_oldest_unpaid_bill_dt + interval  '21'  day) < current_date then 1 else 0 end as soft_dx_window_completed,
        current_date as current_date_analysis
        from max_overdue_first_bill
       
