WITH

FinalTable AS (
    SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Table_DashboardInput_v2`
)

###################################### Soft disconnection new sales ###########################################
,sales_gen AS (
    SELECT ACT_ACCT_CD, MIN(ACT_ACCT_INST_DT) AS FECHA_INSTALACION
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
    GROUP BY 1
)

,first_bill AS (
    SELECT ACT_ACCT_CD, MIN(oldest_unpaid_bill_dt) AS f_bill
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
    GROUP BY 1
    ORDER BY 1
)

,clean_dna AS (
    SELECT *, CASE WHEN LST_PYM_DT < PRIMER_OLDEST_UNPAID THEN NULL ELSE LST_PYM_DT END AS LAST_PAYMENT_DT
    FROM (
        SELECT *,
            FIRST_VALUE(FECHA_EXTRACCION IGNORE NULLS) OVER (PARTITION BY ACT_ACCT_CD ORDER BY FECHA_EXTRACCION) AS PRIMER_DNA,
            FIRST_VALUE(OLDEST_UNPAID_BILL_DT IGNORE NULLS) OVER (PARTITION BY ACT_ACCT_CD ORDER BY FECHA_EXTRACCION) AS PRIMER_OLDEST_UNPAID
        FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
    )
    WHERE ACT_ACCT_INST_DT >= '2021-01-01'
)

,first_bill_dna AS (
    SELECT d.*
    FROM clean_dna d
    INNER JOIN first_bill f
        ON (d.ACT_ACCT_CD = f.ACT_ACCT_CD AND d.oldest_unpaid_bill_dt = f.f_bill)
    WHERE d.ACT_ACCT_INST_DT >= '2021-01-01'
)

,summary AS (
    SELECT 
            act_acct_cd, 
            min(ACT_ACCT_INST_DT) as act_cust_strt_dt,
            DATE_TRUNC(min(ACT_ACCT_INST_DT),MONTH) AS MONTH_INSTALLATION,
            max(oldest_unpaid_bill_dt) as oldest_unpaid_bill_dt,
            max(cast (LAST_PAYMENT_DT as date) ) AS LAST_PAYMENT_DT,
            min(fi_outst_age) as min_fi_outst_age, 
            max(fi_outst_age) as max_fi_outst_age,
            -- ARRAY_AGG(bundle_name IGNORE NULLS order by load_dt LIMIT 1)[OFFSET (0)]as bundle_name,
            -- ARRAY_AGG(pd_vo_prod_nm IGNORE NULLS order by load_dt LIMIT 1)[OFFSET (0)]as pd_vo_prod_nm,
            -- ARRAY_AGG(pd_tv_prod_nm IGNORE NULLS order by load_dt LIMIT 1)[OFFSET (0)]as pd_tv_prod_nm,
            -- ARRAY_AGG(pd_bb_prod_nm IGNORE NULLS order by load_dt LIMIT 1)[OFFSET (0)]as pd_bb_prod_nm,
            -- ARRAY_AGG(pd_mix_nm IGNORE NULLS order by load_dt LIMIT 1)[OFFSET (0)]as pd_mix_nm,
            CASE WHEN (max(fi_outst_age) >=26 ) then 1 ELSE 0 END AS FIRST_BILL_REACHED_SOFT_DX,
            CASE WHEN act_acct_cd IS NOT NULL THEN "New sale" ELSE NULL
            END AS NewSales 
    FROM first_bill_dna
    GROUP BY 1
    ORDER BY ACT_ACCT_CD
    )

,SalesMasterTable AS (
SELECT F.*, FIRST_BILL_REACHED_SOFT_DX, NewSales
FROM FinalTable f LEFT JOIN Summary s
ON safe_cast(s.act_acct_cd AS string)=safe_cast(Fixed_Account AS string) AND safe_cast(MONTH_INSTALLATION as date)=safe_cast(Month as date)
)
SELECT Distinct(Month), COUNT(NewSales) AS NewSales, sum(FIRST_BILL_REACHED_SOFT_DX) AS NewSales_SoftDX, round(sum(FIRST_BILL_REACHED_SOFT_DX)/COUNT(NewSales),2) AS PercentadeSalesSoftdx
FROM SalesMasterTable 
WHERE  NewSales Is not null AND (B_FMC_Segment IN('P1_Fixed','P2','P3','P4') OR E_FMC_Segment IN('P1_Fixed','P2','P3','P4'))
GROUP BY 1


