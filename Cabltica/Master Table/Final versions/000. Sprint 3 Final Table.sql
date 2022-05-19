WITH

FinalTable AS (
    SELECT DISTINCT *,CONCAT(ifnull(B_Plan,""),ifnull(safe_cast(Mobile_ActiveBOM as string),"")) AS B_PLAN_ADJ, CONCAT(ifnull(E_Plan,""),ifnull(safe_cast(Mobile_ActiveEOM as string),"")) AS E_PLAN_ADJ 
    FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Final_Table_DashboardInput_v2`
) 
,FinalTablePlanAdj AS (
    SELECT DISTINCT *, 
    CASE WHEN B_PLAN_ADJ=E_PLAN_ADJ THEN Fixed_Account ELSE NULL END AS no_plan_change_flag
    FROM FinalTable
)


####################################### Involuntarios Never Paid ###############################################
,Installations AS (
    SELECT DATE_TRUNC(FECHA_INSTALACION,MONTH) AS InstallationMonth,act_acct_cd, INSTALLATION_DT,monthsale_Flag

    FROM (
        SELECT ACT_ACCT_CD, MIN(safe_cast(ACT_ACCT_INST_DT as date)) AS FECHA_INSTALACION,DATE(MIN(ACT_ACCT_INST_DT)) AS INSTALLATION_DT,
        CASE WHEN ACT_ACCT_CD IS NOT NULL THEN ACT_ACCT_CD ELSE NULL END AS monthsale_Flag
        FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
        GROUP BY 1
    )
    GROUP BY 1,2,3,4
)

,churn AS (
    SELECT CONTRATOCRM, CHURNTYPEFLAGSO, MAX(FechaChurn) AS FECHA_CHURN, DATE_TRUNC(MAX(FechaChurn),Month) AS CHURN_MONTH
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-03-06_ChurnTypeFlagChurners_D`
    WHERE CHURNTYPEFLAGSO = 'Involuntario'
    GROUP BY 1,2
)

,InstallationChurners AS(
  SELECT V.*,CONTRATOCRM, FECHA_CHURN, CHURNTYPEFLAGSO, CHURN_MONTH, 
  FROM Installations v LEFT JOIN churn ON safe_cast(RIGHT(CONCAT('0000000000',act_acct_cd),10) as string)=safe_cast(RIGHT(CONCAT('0000000000',CONTRATOCRM),10) as string)
)

,never_paid_flag AS (
SELECT DISTINCT d.*, c.act_acct_cd as InstallationAccount,DATE_TRUNC(MIN(safe_cast(ACT_ACCT_INST_DT as date)),MONTH) AS InstallationMonth,CHURNTYPEFLAGSO,CHURN_MONTH,
DATE_DIFF(CST_CHRN_DT,ACT_ACCT_INST_DT,DAY) AS DAYS_WO_PAYMENT,
CASE WHEN DATE_DIFF(CST_CHRN_DT,ACT_ACCT_INST_DT,DAY) <= 119 THEN d.ACT_ACCT_CD ELSE NULL END AS NeverPaid_Flag
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D` d
RIGHT JOIN InstallationChurners c
    ON d.ACT_ACCT_CD = CAST(c.CONTRATOCRM AS INT) AND d.FECHA_EXTRACCION = DATE(c.FECHA_CHURN)
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,
    37,38,40,41,42,43
)


,NeverPaids AS(
  SELECT DISTINCT f.*,InstallationAccount,CHURNTYPEFLAGSO, NeverPaid_Flag,  
  FROM FinalTablePlanAdj f LEFT JOIN never_paid_flag c ON safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string)=safe_cast(RIGHT(CONCAT('0000000000',c.InstallationAccount),10) as string) AND safe_cast(InstallationMonth as string)=Month 
ORDER BY NeverPaid_Flag DESC
)
,NeverPaidMasterTable AS(
SELECT DISTINCT m.*, monthsale_Flag
FROM NeverPaids m LEFT JOIN Installations v ON Month=safe_cast(v.InstallationMonth as string) AND v.act_acct_cd=Fixed_Account 
)

###################################### New Users Issue Calls ####################################################


,LLAMADAS AS(
    SELECT RIGHT(CONCAT('0000000000',CONTRATO),10) AS CONTRATO, FECHA_APERTURA AS FECHA_LLAMADA
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-12_CR_TIQUETES_SERVICIO_2021-01_A_2021-11_D`
    WHERE CLASE IS NOT NULL AND MOTIVO IS NOT NULL AND CONTRATO IS NOT NULL
        AND ESTADO <> "ANULADA"
        AND TIPO <> "GESTION COBRO"
        AND MOTIVO <> "LLAMADA  CONSULTA DESINSTALACION"
)

,INSTALACION_CONTRATOS AS (
    SELECT RIGHT(CONCAT('0000000000',ACT_ACCT_CD),10) AS ACT_ACCT_CD, DATE(MIN(ACT_ACCT_INST_DT)) AS INSTALLATION_DT, DATE_TRUNC(DATE(MIN(ACT_ACCT_INST_DT)),MONTH) AS InstallationMonth
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
    GROUP BY 1
)

,CONTRATOS_LLAMADAS AS (
  SELECT distinct * EXCEPT(FECHA_LLAMADA),
    CASE WHEN DATE_DIFF(MIN(FECHA_LLAMADA),INSTALLATION_DT,DAY) <= 21 THEN ACT_ACCT_CD ELSE NULL END AS EarlyIssue_Flag, -- llamadas hasta 21 días después de la instalación
  FROM INSTALACION_CONTRATOS AS i
  LEFT JOIN LLAMADAS AS l
    ON i.ACT_ACCT_CD = l.CONTRATO
    AND l.FECHA_LLAMADA >= i.INSTALLATION_DT -- el tiquete debe ser después de la instalación
    GROUP BY 1,2,3,4
)

,UserCallDistribution AS (
SELECT DISTINCT ACT_ACCT_CD, DATE_TRUNC(INSTALLATION_DT,MONTH) AS InstallationMonth,EarlyIssue_Flag
FROM CONTRATOS_LLAMADAS
GROUP BY 1,2,3
)

,LlamadasAjustado AS(
  SELECT DISTINCT f.*,EarlyIssue_Flag
  FROM NeverPaidMasterTable f LEFT JOIN CONTRATOS_LLAMADAS c ON safe_cast(RIGHT(CONCAT('0000000000',Fixed_Account),10) as string)=safe_cast(c.CONTRATO as string) AND safe_cast( InstallationMonth as string)=Month 
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
)



,CONTRATOS_TIQUETES AS (
  SELECT DISTINCT * EXCEPT (TIQUETE, FECHA_TIQUETE),
    CASE WHEN DATE_DIFF(FECHA_TIQUETE,INSTALLATION_DT,DAY) BETWEEN 3 AND 49 THEN ACT_ACCT_CD ELSE NULL END AS TechCall_Flag, -- llamadas hasta 7 semanas (49 días) después de la instalación
  FROM INSTALACIONES_TECH AS i
  LEFT JOIN TIQUETES AS l
    ON i.ACT_ACCT_CD = l.CONTRATO
    AND l.FECHA_TIQUETE >= i.INSTALLATION_DT -- el tiquete debe ser después de la instalación
)

,CallsMasterTable AS (
  SELECT DISTINCT f.*, TechCall_Flag FROM LlamadasAjustado f LEFT JOIN CONTRATOS_TIQUETES c ON RIGHT(CONCAT('0000000000',Fixed_Account),10)=RIGHT(CONCAT('0000000000',c.ACT_ACCT_CD),10) AND Month=safe_cast(InstallationMonth as string)

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
SELECT DISTINCT F.*, CASE WHEN NumCalls IS NOT NULL THEN CONTRATO ELSE NULL END AS BillClaim_Flag
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
            CASE WHEN (max(fi_outst_age) >=26 ) then act_acct_cd ELSE NULL END AS SoftDx_Flag,
    FROM first_bill_dna
    GROUP BY 1
    ORDER BY ACT_ACCT_CD
    )
    ,SOFT_DX_INSTALLS AS (
    SELECT a.FECHA_INSTALACION, a.ACT_ACCT_CD, b.SoftDx_Flag FROM sales_gen a LEFT JOIN summary b 
    ON a.FECHA_INSTALACION=b.act_cust_strt_dt AND a.ACT_ACCT_CD=b.ACT_ACCT_CD
    )

,SalesMasterTable AS (
SELECT DISTINCT F.*, SoftDx_Flag,
FROM BillingCallsMasterTable f LEFT JOIN SOFT_DX_INSTALLS s
ON safe_cast(s.act_acct_cd AS string)=safe_cast(Fixed_Account AS string) AND DATE_TRUNC(safe_cast(s.FECHA_INSTALACION as date),MONTH)=safe_cast(Month as date)
)

############################################## Bill shocks #####################################################

,AbsMRC AS (
SELECT *, abs(mrc_change) AS Abs_MRC_Change FROM SalesMasterTable

)
,BillShocks AS (
SELECT DISTINCT *,
CASE
WHEN Abs_MRC_Change>(TOTAL_B_MRC*(.05)) AND B_PLAN_ADJ=E_PLAN_ADJ AND no_plan_change_flag is not null THEN act_acct_cd ELSE NULL END AS increase_flag
FROM AbsMRC
)

########################################### Outlier Installations ################################################

,INSTALACIONES_OUTLIER AS (
    SELECT DISTINCT RIGHT(CONCAT('0000000000',ACT_ACCT_CD),10) AS ACT_ACCT_CD, DATE(MIN(ACT_ACCT_INST_DT)) AS INSTALLATION_DT, DATE_TRUNC(DATE(MIN(ACT_ACCT_INST_DT)),MONTH) AS InstallationMonth
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-04-20_Historical_CRM_ene_2021_mar_2022_D`
    GROUP BY 1
)

,tiempo_instalacion AS (
    SELECT RIGHT(CONCAT('0000000000',NOMBRE_CONTRATO),10) AS NOMBRE_CONTRATO,DATE_TRUNC(SAFE_CAST(FECHA_FINALIZACION AS DATE),MONTH) AS InstallationMonth,
        TIMESTAMP_DIFF(FECHA_FINALIZACION,FECHA_APERTURA,DAY) AS DIAS_INSTALACION,
        CASE WHEN TIMESTAMP_DIFF(FECHA_FINALIZACION,FECHA_APERTURA,DAY) >= 6 THEN NOMBRE_CONTRATO ELSE NULL END AS long_install_flag,
        CASE WHEN NOMBRE_CONTRATO IS NOT NULL THEN "Installation" ELSE NULL END AS Installations
    FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.2022-01-12_CR_ORDENES_SERVICIO_2021-01_A_2021-11_D`
    WHERE
        TIPO_ORDEN = 'INSTALACION' 
        AND ESTADO = 'FINALIZADA'
        AND TIPO_CLIENTE IN ("PROGRAMA HOGARES CONECTADOS", "RESIDENCIAL", "EMPLEADO")
)

,Installations_6_days AS (
    SELECT ACT_ACCT_CD, a.InstallationMonth, b.long_install_flag FROM INSTALACIONES_OUTLIER a LEFT JOIN tiempo_instalacion b
    ON safe_cast(NOMBRE_CONTRATO as string)=safe_cast(ACT_ACCT_CD as string) AND a.InstallationMonth=b.InstallationMonth
)


,OutliersMasterTable AS (
    SELECT DISTINCT f.*, long_install_flag
    FROM BillShocks f LEFT JOIN Installations_6_days b ON safe_cast(b.ACT_ACCT_CD as string)=RIGHT(CONCAT('0000000000',Fixed_Account),10) AND Month=safe_cast(InstallationMonth AS string)
)

######################################## Sales Channel ######################################################

,SalesChannel as (
SELECT distinct contrato,Categoria_Canal,subcanal_venta, case 
WHEN formato_fecha like "%2022" then parse_date("%d/%m/%Y",formato_fecha) else safe_cast(formato_fecha as date) end as FechaAdj
FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220510-Altas_ene_2021_abr_2022` 
group by 1,2,3,formato_fecha
)
,FechaFin AS (
    Select distinct  *, min(FechaAdj) AS FechaFinal From SalesChannel
    group by 1,2,3,4

)

,SalesChannelsInstallations as (
select distinct act_acct_cd, contrato,InstallationMonth, Categoria_Canal, subcanal_venta,
from Installations left join FechaFin
on RIGHT(CONCAT('0000000000',CONTRATO),10)=RIGHT(CONCAT('0000000000',ACT_ACCT_CD),10) and InstallationMonth=date_trunc(FechaFinal,month)

)
,ChannelsMasterTable AS (
    select distinct f.*, categoria_canal, subcanal_venta, from OutliersMasterTable f left join SalesChannelsInstallations
    on RIGHT(CONCAT('0000000000',CONTRATO),10)=RIGHT(CONCAT('0000000000',Fixed_Account),10) AND safe_cast(InstallationMonth as string)=Month
)
,ChannelAndSubchannel AS (
  SELECT distinct *, CASE
  WHEN Subcanal_venta="INHOUSE FORMULARIO" OR Subcanal_venta="INHOUSE CHAT" OR Subcanal_venta="INHOUSE WP" OR Subcanal_venta="ITS FORMULARIOS" OR
  Subcanal_venta="ITS WP" OR Subcanal_venta="ITS WP MOVISTAR" OR Subcanal_venta="ITS CHAT" OR Subcanal_venta="INHOUSE WP MOVISTAR" Then "Digital"
  WHEN Subcanal_venta="OUTBOUND CAC" OR Subcanal_venta="OUTBOUND TELEVENTAS" OR Subcanal_venta="OUTBOUND SUCURSALES" OR Subcanal_venta="ITS OUTBOUND" OR Subcanal_venta="ITS CRIANZA" OR Subcanal_venta="ITS CORREO" 
  OR Subcanal_venta="OUTBOUND ALBUFERA" OR Subcanal_venta="OUTBOUND DATA 506" OR Subcanal_venta="OUTBOUND TICO CEL" OR Subcanal_venta="OUTBOUND TEL MOVIL" OR Subcanal_venta="OUTBOUND RED ALO" THEN "Televentas-Outbound"
  WHEN Subcanal_venta="INHOUSE WP" OR Subcanal_venta="INHOUSE CHAT" OR Subcanal_venta="INHOUSE FORMULARIO" OR Subcanal_venta="TELE VENTAS" OR Subcanal_venta="ITS FORMULARIOS" OR Subcanal_venta="ITS INBOUND" THEN "Televentas-Inbound"
  WHEN Subcanal_venta="VENTAS RESIDENCIALES" THEN "D2D-Local"
  WHEN Subcanal_venta="CTVS" OR Subcanal_venta="" OR Subcanal_venta="CAMBIOS POSITIVOS" OR Subcanal_venta="UPGRADE SOLUTIONS" OR Subcanal_venta="CRC EN LINEA SA" OR Subcanal_venta="CONECTIVIDAD CR" THEN "D2D-Third Party"
  Else categoria_canal END AS SalesChannelAdjusted FROM ChannelsMasterTable
)


,FinalSalesChannel AS(
select DISTINCT * except(categoria_canal, subcanal_venta), CASE
WHEN SalesChannelAdjusted="Digital"  THEN "Digital"
WHEN SalesChannelAdjusted="Televentas-Outbound"  THEN "Televentas-Outbound"
WHEN SalesChannelAdjusted="Televentas-Inbound"  THEN "Televentas-Inbound"
WHEN SalesChannelAdjusted="D2D-Local"  THEN "D2D-Local"
WHEN SalesChannelAdjusted="D2D-Third Party"  THEN "D2D-Third Party"
WHEN SalesChannelAdjusted="Agentes Autorizados" OR SalesChannelAdjusted="Ventas Residenciales" THEN "D2D"
WHEN SalesChannelAdjusted="Televentas" OR SalesChannelAdjusted="ITS" THEN "Telesales"
WHEN SalesChannelAdjusted="*No Definido*" OR SalesChannelAdjusted="Sin datos" THEN "Undefined/ No Data"
WHEN SalesChannelAdjusted="Oficina" THEN "Retail"
WHEN SalesChannelAdjusted="NETCOM" OR SalesChannelAdjusted="Hoteles/Condominios" OR SalesChannelAdjusted="Ventas Empresariales" THEN "Other"
ELSE NULL END AS Categoria_canal
from ChannelAndSubchannel

)
################################################# Excel Table ###############################################

select distinct Month, E_FinalTechFlag, E_FMC_Segment,E_FMCType, count(distinct fixed_account) as activebase, 
count(distinct monthsale_flag) as Sales, count(distinct SoftDx_Flag) as Soft_Dx, 
count(distinct NeverPaid_Flag) as NeverPaid, count(distinct long_install_flag) as Long_installs, 
count (distinct increase_flag) as MRC_Increases, count (distinct no_plan_change_flag) as NoPlan_Changes,
count(distinct EarlyIssue_Flag) as EarlyIssueCall, count(distinct TechCall_Flag) as TechCalls,
count(distinct BillClaim_Flag) as BillClaim,categoria_canal


from FinalSalesChannel
Group by 1,2,3,4,15
Order by 1 desc, 2,3,4
