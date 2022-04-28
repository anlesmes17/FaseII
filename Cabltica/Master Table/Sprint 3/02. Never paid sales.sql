WITH

FinalTable AS (
    SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Table_DashboardInput_v2`
) 


####################################### Involuntarios Never Paid ###############################################
,ventas AS (
    SELECT DATE_TRUNC(FECHA_INSTALACION,MONTH) AS InstallationMonth,act_acct_cd--, COUNT(DISTINCT ACT_ACCT_CD) AS INSTALLATIONS
    FROM (
        SELECT ACT_ACCT_CD, MIN(safe_cast(ACT_ACCT_INST_DT as date)) AS FECHA_INSTALACION
        FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
        GROUP BY 1
    )
    GROUP BY 1,2
)

,churn AS (
    SELECT CONTRATOCRM, CHURNTYPEFLAGSO, MAX(FechaChurn) AS FECHA_CHURN, 
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-03-06_ChurnTypeFlagChurners_D`
    WHERE CHURNTYPEFLAGSO = 'Involuntario'
    GROUP BY 1,2
)
,InstallationChurners AS(
  SELECT V.*,CONTRATOCRM, FECHA_CHURN, CHURNTYPEFLAGSO
  FROM ventas v LEFT JOIN churn ON safe_cast(RIGHT(CONCAT('0000000000',act_acct_cd),10) as float64)=safe_cast(RIGHT(CONCAT('0000000000',CONTRATOCRM),10) as float64)
-- WHERE  /*AND act_acct_cd=1335050*/  InstallationMonth="2021-02-01" AND CONTRATOCRM IS NOT NULL
 --ORDER BY CHURNTYPEFLAGSO DESC
)/*
SELECT * FROM InstallationChurners
WHERE InstallationMonth="2022-02-01"
*/

--,never_paid_flag AS (
SELECT d.*, c.act_acct_cd as InstallationAccount,InstallationMonth,CHURNTYPEFLAGSO,
DATE_DIFF(CST_CHRN_DT,ACT_ACCT_INST_DT,DAY) AS DAYS_WO_PAYMENT, 
CASE WHEN DATE_DIFF(CST_CHRN_DT,ACT_ACCT_INST_DT,DAY) <= 119 THEN d.ACT_ACCT_CD ELSE NULL END AS NEVER_PAID_CUSTOMER
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D` d
INNER JOIN InstallationChurners c
    ON d.ACT_ACCT_CD = c.act_acct_cd AND d.FECHA_EXTRACCION = DATE(c.FECHA_CHURN)
    WHERE c.act_acct_cd is not null 
    ORDER BY INSTALLATIONMONTH DESC
    --GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,
    --38,39,40
)


,NeverPaidMasterTable AS(
  SELECT f.*,InstallationAccount,InstallationMonth,CHURNTYPEFLAGSO, NEVER_PAID_CUSTOMER,  
  FROM FinalTable f LEFT JOIN never_paid_flag c ON safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string)=safe_cast(RIGHT(CONCAT('0000000000',c.act_acct_cd),10) as string) AND safe_cast( InstallationMonth as string)=Month 
)


,Sales AS(
SELECT Distinct(Month), COUNT(distinct Fixed_Account) AS NumSales,FROM NeverPaidMasterTable 
WHERE InstallationMonth IS NOT NULL AND (B_FMC_Segment IN('P1_Fixed','P2','P3','P4') OR E_FMC_Segment IN('P1_Fixed','P2','P3','P4'))
GROUP BY 1
)
,NumberOfNeverPaids AS (
SELECT Distinct(Month), COUNT(distinct Fixed_account) AS NumNeverPaids,FROM NeverPaidMasterTable 
WHERE NEVER_PAID_CUSTOMER IS NOT NULL AND (B_FMC_Segment IN('P1_Fixed','P2','P3','P4') OR E_FMC_Segment IN('P1_Fixed','P2','P3','P4'))
GROUP BY 1
)

SELECT C.*, NumNeverPaids, round(NumNeverPaids/NumSales,3) AS PercentageCallers
FROM Sales c LEFT JOIN NumberOfNeverPaids n ON c.Month=n.Month
