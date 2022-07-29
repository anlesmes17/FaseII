wITH

FMC_Table AS (
SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-07-14_Cabletica_FMC_DashboardInput`
)



######################################### Voluntary Funnel Personas #############################################

,SolicitudDesconexiones AS(
  SELECT DISTINCT safe_cast(DATE_TRUNC(safe_cast(CONCAT(A__oMes,'-01') as date),Month) as string) AS EventMonth, contrato
  FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220713_Retencion2021_2022`
)

,Localizados AS(
  SELECT DISTINCT safe_cast(DATE_TRUNC(safe_cast(CONCAT(A__oMes,'-01') as date),Month) as string) AS EventMonth, contrato
  FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220713_Retencion2021_2022`
  WHERE Contactabilidad="CONTACTO EFECTIVO"
)

,Retenidos AS(
  SELECT DISTINCT safe_cast(DATE_TRUNC(safe_cast(CONCAT(A__oMes,'-01') as date),Month) as string) AS EventMonth, Contrato
  FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220713_Retencion2021_2022`
  WHERE Soluci__n="RETENIDO"
)

,SolicitudesMasterTable AS(
  SELECT a.*,b.contrato AS SolicitudDesconexiones FROM FMC_Table a LEFT JOIN SolicitudDesconexiones b
  ON RIGHT(CONCAT('0000000000',CONTRATO),10)=RIGHT(CONCAT('0000000000',Fixed_Account),10) AND Month=EventMonth
)

,LocalizadosMasterTable AS(
  SELECT a.*,b.contrato AS Localizados FROM SolicitudesMasterTable a LEFT JOIN Localizados b
  ON RIGHT(CONCAT('0000000000',CONTRATO),10)=RIGHT(CONCAT('0000000000',Fixed_Account),10) AND Month=EventMonth
)

,RetenidosMasterTable AS(
  SELECT a.*,b.contrato AS Retenidos FROM LocalizadosMasterTable a LEFT JOIN Retenidos b
  ON RIGHT(CONCAT('0000000000',CONTRATO),10)=RIGHT(CONCAT('0000000000',Fixed_Account),10) AND Month=EventMonth
)

,RGUs as(
  Select *,
  CASE WHEN Fixed_Month IS NOT NULL THEN B_TotalRGUs ELSE NULL END AS ActiveBase_RGUs,
  CASE WHEN Fixed_Month IS NOT NULL and SolicitudDesconexiones IS NOT NULL THEN B_TotalRGUs ELSE NULL END AS SolicitudDx_RGUs,
  CASE WHEN Fixed_Month IS NOT NULL and Localizados IS NOT NULL THEN B_TotalRGUs ELSE NULL END AS Localizados_RGUs,
  CASE WHEN Fixed_Month IS NOT NULL and Retenidos IS NOT NULL THEN B_TotalRGUs ELSE NULL END AS Retenidos_RGUs,
  FROM RetenidosMasterTable
)



############################################## CSV File #################################################

select distinct Month,-- E_FinalTechFlag, E_FMC_Segment,E_FMCType,E_TenureFinalFlag, 
count(distinct fixed_account) as activebase,round(sum(ActiveBase_RGUs),0) as ActiveBase_RGUs,
count(distinct SolicitudDesconexiones) AS SolicitudDx,round(sum(SolicitudDx_RGUs),0) as SolicitudDx_RGUs,
count(distinct localizados) AS Localizados,round(sum(Localizados_RGUs),0) as Localizados_RGUs,
count(distinct Retenidos) AS Retenidos,round(sum(Retenidos_RGUs),0) as Retenidos_RGUs,
--B_FinalTechFlag, B_FMC_Segment,B_FMCType,B_TenureFinalFlag
FROM RGUs
Group by 1--,2,3,4,5,10,11,12,13
order by 1
