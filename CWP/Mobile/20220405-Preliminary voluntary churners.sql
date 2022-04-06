    SELECT dt, Count(*) FROM  `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.cwp_info_dna_postpaid_history`
    WHERE VOLUNTARY_FLG="1.0" AND BIZ_UNIT_D="B2C"  AND INV_PAYMT_DT<>"nan" 
    AND (STOPDATE_ACCOUNTNO>"2022-31-01" OR STOPDATE_ACCOUNTNO="nan")
    GROUP BY dt
    ORDER BY dt
