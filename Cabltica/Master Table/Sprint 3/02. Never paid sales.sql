WITH

FinalTable AS (
    SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Table_DashboardInput_v2`
) 


####################################### Involuntarios Never Paid ###############################################
,ventas AS (
    SELECT DATE_TRUNC(FECHA_INSTALACION,MONTH) AS InstallationMonth,act_acct_cd
    FROM (
        SELECT ACT_ACCT_CD, MIN(safe_cast(ACT_ACCT_INST_DT as date)) AS FECHA_INSTALACION
        FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
        GROUP BY 1
    )
    GROUP BY 1,2
)

,churn AS (
    SELECT CONTRATOCRM, CHURNTYPEFLAGSO, MAX(FechaChurn) AS FECHA_CHURN, DATE_TRUNC(MAX(FechaChurn),Month) AS CHURN_MONTH
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-03-06_ChurnTypeFlagChurners_D`
    WHERE CHURNTYPEFLAGSO = 'Involuntario'
    GROUP BY 1,2
)

,InstallationChurners AS(
  SELECT V.*,CONTRATOCRM, FECHA_CHURN, CHURNTYPEFLAGSO, CHURN_MONTH
  FROM ventas v LEFT JOIN churn ON safe_cast(RIGHT(CONCAT('0000000000',act_acct_cd),10) as string)=safe_cast(RIGHT(CONCAT('0000000000',CONTRATOCRM),10) as string)

)

,never_paid_flag AS (
SELECT d.*, c.act_acct_cd as InstallationAccount,InstallationMonth,CHURNTYPEFLAGSO,CHURN_MONTH,
DATE_DIFF(CST_CHRN_DT,ACT_ACCT_INST_DT,DAY) AS DAYS_WO_PAYMENT, 
CASE WHEN DATE_DIFF(CST_CHRN_DT,ACT_ACCT_INST_DT,DAY) <= 119 THEN d.ACT_ACCT_CD ELSE NULL END AS NEVER_PAID_CUSTOMER
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D` d
RIGHT JOIN InstallationChurners c
    ON d.ACT_ACCT_CD = c.act_acct_cd AND d.FECHA_EXTRACCION = DATE(c.FECHA_CHURN)

    
)


,NeverPaidMasterTable AS(
  SELECT f.*,InstallationAccount,InstallationMonth,CHURNTYPEFLAGSO, NEVER_PAID_CUSTOMER,  
  FROM FinalTable f LEFT JOIN never_paid_flag c ON safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string)=safe_cast(RIGHT(CONCAT('0000000000',c.InstallationAccount),10) as string)  AND safe_cast(CHURN_MONTH as string)=Month 
)
,NewInstallations AS(
SELECT m.*, v.InstallationMonth AS NewInstallation
FROM NeverPaidMasterTable m LEFT JOIN ventas v ON Month=safe_cast(v.InstallationMonth as string) AND v.act_acct_cd=Fixed_Account 
)

,Sales AS(
SELECT Distinct(Month), COUNT(distinct Fixed_Account) AS NumSales,FROM NewInstallations 
WHERE NewInstallation IS NOT NULL AND (B_FMC_Segment IN('P1_Fixed','P2','P3','P4') OR E_FMC_Segment IN('P1_Fixed','P2','P3','P4'))
GROUP BY 1
)
,NumberOfNeverPaids AS (
SELECT Distinct(Month), COUNT(distinct Fixed_account) AS NumNeverPaids,FROM NewInstallations
WHERE NEVER_PAID_CUSTOMER IS NOT NULL AND (B_FMC_Segment IN('P1_Fixed','P2','P3','P4') OR E_FMC_Segment IN('P1_Fixed','P2','P3','P4'))
GROUP BY 1
)

SELECT C.*, NumNeverPaids, round(NumNeverPaids/NumSales,3) AS PercentageCallers
FROM Sales c LEFT JOIN NumberOfNeverPaids n ON c.Month=n.Month
