WITH

FinalTable AS (
    SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Table_DashboardInput_v2`
) 


####################################### Involuntarios Never Paid ###############################################
,Installations AS (
    SELECT DATE_TRUNC(FECHA_INSTALACION,MONTH) AS InstallationMonth,act_acct_cd, INSTALLATION_DT

    FROM (
        SELECT ACT_ACCT_CD, MIN(safe_cast(ACT_ACCT_INST_DT as date)) AS FECHA_INSTALACION,DATE(MIN(ACT_ACCT_INST_DT)) AS INSTALLATION_DT
        FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
        GROUP BY 1
    )
    GROUP BY 1,2,3
)

,churn AS (
    SELECT CONTRATOCRM, CHURNTYPEFLAGSO, MAX(FechaChurn) AS FECHA_CHURN, DATE_TRUNC(MAX(FechaChurn),Month) AS CHURN_MONTH
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-03-06_ChurnTypeFlagChurners_D`
    WHERE CHURNTYPEFLAGSO = 'Involuntario'
    GROUP BY 1,2
)

,InstallationChurners AS(
  SELECT V.*,CONTRATOCRM, FECHA_CHURN, CHURNTYPEFLAGSO, CHURN_MONTH
  FROM Installations v LEFT JOIN churn ON safe_cast(RIGHT(CONCAT('0000000000',act_acct_cd),10) as string)=safe_cast(RIGHT(CONCAT('0000000000',CONTRATOCRM),10) as string)

)

,never_paid_flag AS (
SELECT d.*, c.act_acct_cd as InstallationAccount,DATE_TRUNC(MIN(safe_cast(ACT_ACCT_INST_DT as date)),MONTH) AS InstallationMonth,CHURNTYPEFLAGSO,CHURN_MONTH,
DATE_DIFF(CST_CHRN_DT,ACT_ACCT_INST_DT,DAY) AS DAYS_WO_PAYMENT, 
CASE WHEN DATE_DIFF(CST_CHRN_DT,ACT_ACCT_INST_DT,DAY) <= 119 THEN d.ACT_ACCT_CD ELSE NULL END AS NEVER_PAID_CUSTOMER
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D` d
RIGHT JOIN InstallationChurners c
    ON d.ACT_ACCT_CD = CAST(c.CONTRATOCRM AS INT) AND d.FECHA_EXTRACCION = DATE(c.FECHA_CHURN)
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,
    37,38,40,41,42,43

    
)


,NeverPaidMasterTable AS(
  SELECT f.*,InstallationAccount,CHURNTYPEFLAGSO, NEVER_PAID_CUSTOMER,  
  FROM FinalTable f LEFT JOIN never_paid_flag c ON safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string)=safe_cast(RIGHT(CONCAT('0000000000',c.InstallationAccount),10) as string) AND safe_cast(InstallationMonth as string)=Month 
ORDER BY NEVER_PAID_CUSTOMER DESC
)
,NewInstallations AS(
SELECT m.*, v.InstallationMonth AS NewInstallation
FROM NeverPaidMasterTable m LEFT JOIN Installations v ON Month=safe_cast(v.InstallationMonth as string) AND v.act_acct_cd=Fixed_Account 
)

###################################### New Users Issue Calls ####################################################


,LLAMADAS AS(
    SELECT RIGHT(CONCAT('0000000000',CONTRATO),10) AS CONTRATO, FECHA_APERTURA AS FECHA_LLAMADA, TIQUETE_ID
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-12_CR_TIQUETES_SERVICIO_2021-01_A_2021-11_D`
    WHERE CLASE IS NOT NULL AND MOTIVO IS NOT NULL AND CONTRATO IS NOT NULL
        AND ESTADO <> "ANULADA"
        AND TIPO <> "GESTION COBRO"
        AND MOTIVO <> "LLAMADA  CONSULTA DESINSTALACION"
)

,INSTALACION_CONTRATOS AS (
    SELECT RIGHT(CONCAT('0000000000',ACT_ACCT_CD),10) AS ACT_ACCT_CD, DATE(MIN(ACT_ACCT_INST_DT)) AS INSTALLATION_DT,
    CASE WHEN act_acct_cd IS NOT NULL THEN "Installation" ELSE NULL
    END AS Installations
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
    GROUP BY 1,3
)

,CONTRATOS_LLAMADAS AS (
  SELECT *,
    CASE WHEN DATE_DIFF(FECHA_LLAMADA,INSTALLATION_DT,DAY) <= 21 THEN ACT_ACCT_CD ELSE NULL END AS LLAMADA_EarlyIssue_21D, -- llamadas hasta 21 días después de la instalación
    CASE WHEN DATE_DIFF(FECHA_LLAMADA,INSTALLATION_DT,DAY) <= 49 THEN ACT_ACCT_CD ELSE NULL END AS LLAMADA_7W, -- llamadas hasta 7 semanas (49 días) después de la instalación
    CASE WHEN DATE_DIFF(FECHA_LLAMADA,INSTALLATION_DT,MONTH) BETWEEN 2 AND 6 THEN ACT_ACCT_CD ELSE NULL END AS LLAMADA_2M_6M -- llamadas entre 2 y 6 meses después de la instalación
  FROM INSTALACION_CONTRATOS AS i
  LEFT JOIN LLAMADAS AS l
    ON i.ACT_ACCT_CD = l.CONTRATO
    AND l.FECHA_LLAMADA >= i.INSTALLATION_DT -- el tiquete debe ser después de la instalación
)

,UserCallDistribution AS (
SELECT DISTINCT ACT_ACCT_CD, DATE_TRUNC(INSTALLATION_DT,MONTH) AS InstallationMonth,Installations, COUNT(LLAMADA_EarlyIssue_21D) AS LLAMADAS_EarlyIssue_21D, COUNT(LLAMADA_7W) AS Llamadas7semanas, COUNT(LLAMADA_2M_6M) AS Llamadas2a6meses
FROM CONTRATOS_LLAMADAS
GROUP BY 1,2,3
)

,LlamadasMasterTable AS(
  SELECT f.*,Installations, LLAMADAS_EarlyIssue_21D, Llamadas7semanas, Llamadas2a6meses,  
  FROM NewInstallations f LEFT JOIN UserCallDistribution c ON safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string)=safe_cast(c.ACT_ACCT_CD as string) AND safe_cast( InstallationMonth as string)=Month 
)
,LlamadasAjustado AS(
SELECT l.* except(LLAMADAS_EarlyIssue_21D, Llamadas7semanas, Llamadas2a6meses),
CASE WHEN LLAMADAS_EarlyIssue_21D =0 THEN NULL ELSE LLAMADAS_EarlyIssue_21D END AS LLAMADAS_EarlyIssue_21D,
CASE WHEN Llamadas7semanas=0 THEN NULL ELSE Llamadas7semanas END AS Llamadas7semanas,
CASE WHEN Llamadas2a6meses=0 THEN NULL ELSE Llamadas2a6meses END AS Llamadas2a6meses
FROM LlamadasMasterTable l
)

################################## New users early tech tickets ###########################################

,TIQUETES AS(
    SELECT DISTINCT RIGHT(CONCAT('0000000000',CONTRATO),10) AS CONTRATO, FECHA_APERTURA AS FECHA_TIQUETE, DATE_TRUNC(FECHA_APERTURA,MONTH) AS MES_TIQUETE, TIQUETE
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-19_CR_TIQUETES_AVERIAS_2021-01_A_2021-11_D`
    WHERE 
        CLASE IS NOT NULL AND MOTIVO IS NOT NULL AND CONTRATO IS NOT NULL
        AND ESTADO <> "ANULADA"
        AND TIQUETE NOT IN (SELECT DISTINCT TIQUETE FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-19_CR_TIQUETES_AVERIAS_2021-01_A_2021-11_D` WHERE CLIENTE LIKE '%SIN PROBLEMA%')
)

,INSTALACIONES_TECH AS (
    SELECT DISTINCT RIGHT(CONCAT('0000000000',ACT_ACCT_CD),10) AS ACT_ACCT_CD, DATE(MIN(ACT_ACCT_INST_DT)) AS INSTALLATION_DT, DATE_TRUNC(DATE(MIN(ACT_ACCT_INST_DT)),MONTH) AS InstallationMonth
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
    GROUP BY 1
    --HAVING DATE_TRUNC(INSTALLATION_DT, MONTH) = '2022-01-01'  -- solo instalaciones en enero
)



,CONTRATOS_TIQUETES AS (
  SELECT DISTINCT * EXCEPT (TIQUETE, FECHA_TIQUETE),
    CASE WHEN DATE_DIFF(FECHA_TIQUETE,INSTALLATION_DT,DAY) BETWEEN 3 AND 49 THEN ACT_ACCT_CD ELSE NULL END AS LLAMADA_7W, -- llamadas hasta 7 semanas (49 días) después de la instalación
    CASE WHEN INSTALLATION_DT IS NOT NULL THEN "Installation" ELSE NULL END AS Installations
  FROM INSTALACIONES_TECH AS i
  LEFT JOIN TIQUETES AS l
    ON i.ACT_ACCT_CD = l.CONTRATO
    AND l.FECHA_TIQUETE >= i.INSTALLATION_DT -- el tiquete debe ser después de la instalación
)

,CallsMasterTable AS (
  SELECT DISTINCT f.*, LLAMADA_7W FROM LlamadasAjustado f LEFT JOIN CONTRATOS_TIQUETES c ON RIGHT(CONCAT('0000000000',Fixed_Account),10)=RIGHT(CONCAT('0000000000',c.ACT_ACCT_CD),10) AND Month=safe_cast(InstallationMonth as string)

)

############################################# Bill Claims #####################################################

,CALLS AS (
SELECT RIGHT(CONCAT('0000000000',CONTRATO),10) AS CONTRATO, DATE_TRUNC(FECHA_APERTURA, MONTH) AS Call_Month, TIQUETE_ID
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-12_CR_TIQUETES_SERVICIO_2021-01_A_2021-11_D`
    WHERE 
        CLASE IS NOT NULL AND MOTIVO IS NOT NULL AND CONTRATO IS NOT NULL
        AND ESTADO <> "ANULADA"
        AND TIPO <> "GESTION COBRO"
        AND MOTIVO NOT IN ("LLAMADA  CONSULTA DESINSTALACION","CONSULTAS DE INSTALACIONES")
        AND MOTIVO = "CONSULTAS DE FACTURACION O COBRO" -- billing
)
,CallsPerUser AS (
    SELECT DISTINCT CONTRATO, Call_Month, Count(DISTINCT TIQUETE_ID) AS NumCalls
    FROM CALLS
    GROUP BY CONTRATO, Call_Month
)

,BillingCallsMasterTable AS (
SELECT F.*, NumCalls
FROM CallsMasterTable f LEFT JOIN CallsPerUser 
ON safe_cast(CONTRATO AS string)=safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) AS string) AND safe_cast(Call_Month as string)=Month
)

######################################### Soft dx first bill #####################################################


,sales_gen AS (
    SELECT DISTINCT ACT_ACCT_CD, MIN(ACT_ACCT_INST_DT) AS FECHA_INSTALACION
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
    SELECT DISTINCT
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
            CASE WHEN (max(fi_outst_age) >=26 ) then 1 ELSE NULL END AS FIRST_BILL_REACHED_SOFT_DX,
            CASE WHEN act_acct_cd IS NOT NULL THEN "New sale" ELSE NULL
            END AS NewSales 
    FROM first_bill_dna
    GROUP BY 1
    ORDER BY ACT_ACCT_CD
    )
    ,SOFT_DX_INSTALLS AS (
    SELECT a.FECHA_INSTALACION, a.ACT_ACCT_CD, b.FIRST_BILL_REACHED_SOFT_DX FROM sales_gen a LEFT JOIN summary b 
    ON a.FECHA_INSTALACION=b.act_cust_strt_dt AND a.ACT_ACCT_CD=b.ACT_ACCT_CD
    )

,SalesMasterTable AS (
SELECT F.*, FIRST_BILL_REACHED_SOFT_DX,
FROM BillingCallsMasterTable f LEFT JOIN SOFT_DX_INSTALLS s
ON safe_cast(s.act_acct_cd AS string)=safe_cast(Fixed_Account AS string) AND DATE_TRUNC(safe_cast(s.FECHA_INSTALACION as date),MONTH)=safe_cast(Month as date)
)

############################################## Bill shocks #####################################################

,AbsMRC AS (
SELECT *, abs(mrc_change) AS Abs_MRC_Change FROM SalesMasterTable
WHERE (B_FMC_Segment IN('P1_Fixed','P2','P3','P4') OR E_FMC_Segment IN('P1_Fixed','P2','P3','P4'))
)
,BillShocks AS (
SELECT *,
CASE
WHEN Abs_MRC_Change>(TOTAL_B_MRC*(.05)) AND B_PLAN=E_PLAN THEN "Bill Schock" ELSE NULL END AS BillShocksKPI
FROM AbsMRC
)

,BaseNoChanges AS (
SELECT Month, COUNT(DISTINCT Fixed_Account) AS AccountsNoChanges FROM BillShocks
GROUP BY 1
)
,CountBillShocks AS (
  SELECT Month, COUNT(DISTINCT Fixed_Account) AS BillChanges 
  FROM BillShocks
  WHERE BillShocksKPI IS NOT NULL
  GROUP BY 1
)
SELECT b.Month, AccountsNoChanges, BillChanges
FROM BaseNoChanges b LEFT JOIN CountBillShocks c ON b.Month=c.Month
ORDER BY Month

########################################### Mounting Bills ######################################################
