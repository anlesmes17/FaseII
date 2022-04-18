WITH 

Fixed_Base AS(
  SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Fixed_DashboardInput`
)

,Mobile_Base AS(
  SELECT * FROM `gcp-bia-tmps-vtr-dev-01.lla_temp_dna_tables.2022-04-18_Cabletica_Mobile_DashboardInput`
)

###################################################### FMC Match############################################################################################

/*,FMC_Base AS (
  SELECT DISTINCT Fixed_Month, Fixed_Account, B_Contrato, Mobile_Month 
  FROM Fixed_Base Inner Join Mobile_Base 
  ON Fixed_Month=Mobile_Month AND B_contrato=Fixed_Account
)

,FixedFMCMatch AS (
  SELECT DISTINCT Fixed_Account, Fixed_Month, contrato, Fixed_Month
  FROM Fixed_Base a LEFT JOIN FMC_Base b ON Fixed_Account=Contrato AND a.Fixed_Month=b.Fixed_Month
)

,MobileFMCMatch AS (
  SELECT DISTINCT Mobile_Account, Mobile_Month, contrato, Mobile_Month
  FROM Mobile_Base a LEFT JOIN FMC_Base b ON Mobile_Account=Contrato AND a.Mobile_Month=b.Mobile_Month
)*/


############################################## Join Fixed Mobile ################################################

,FullCustomerBase AS(
SELECT DISTINCT
CASE WHEN (Fixed_Account IS NOT NULL AND Mobile_Account IS NOT NULL) OR (Fixed_Account IS NOT NULL AND Mobile_Account IS NULL) THEN safe_cast(Fixed_Month as string)
      WHEN (Fixed_Account IS NULL AND Mobile_Account IS NOT NULL) THEN safe_cast(Mobile_Month as string)
  END AS Month,
CASE WHEN (Fixed_Account IS NOT NULL AND Mobile_Account IS NOT NULL) OR (Fixed_Account IS NOT NULL AND Mobile_Account IS NULL) THEN safe_cast(Fixed_Account as string)
      WHEN (Fixed_Account IS NULL AND Mobile_Account IS NOT NULL) THEN safe_cast(Mobile_Account as string)
  END AS Final_Account,
CASE WHEN (ActiveBOM =1 AND Mobile_ActiveBOM=1) or (ActiveBOM=1 AND (Mobile_ActiveBOM=0 or Mobile_ActiveBOM IS NULL)) or ((ActiveBOM=0 OR ActiveBOM IS NULL) AND Mobile_ActiveBOM=1) THEN 1
ELSE 0 END AS Final_BOM_ActiveFlag,
CASE WHEN (ActiveEOM =1 AND Mobile_ActiveEOM=1) or (ActiveEOM=1 AND (Mobile_ActiveEOM=0 or Mobile_ActiveEOM IS NULL)) or ((ActiveEOM=0 OR ActiveEOM IS NULL) AND Mobile_ActiveEOM=1) THEN 1
ELSE 0 END AS Final_EOM_ActiveFlag,
CASE 
WHEN (Fixed_Account is not null and Mobile_Account is not null and ActiveBOM = 1 and Mobile_ActiveBOM = 1) THEN "FMC soft"
WHEN (Fixed_Account IS NOT NULL AND ActiveBOM=1 AND (Mobile_ActiveBOM = 0 OR Mobile_ActiveBOM IS NULL)) THEN "Fixed Only"
WHEN ((Mobile_Account IS NOT NULL AND Mobile_ActiveBOM=1 AND (ActiveBOM = 0 OR ActiveBOM IS NULL))) THEN "Mobile Only"
  END AS B_FMC_Status,
CASE 
WHEN (Fixed_Account is not null and Mobile_Account is not null and ActiveEOM = 1 and Mobile_ActiveEOM = 1) THEN "FMC soft"
WHEN (Fixed_Account IS NOT NULL AND ActiveEOM=1 AND (Mobile_ActiveEOM = 0 OR Mobile_ActiveEOM IS NULL)) THEN "Fixed Only"
WHEN (Mobile_Account IS NOT NULL AND Mobile_ActiveEOM=1 AND (ActiveEOM = 0 OR ActiveEOM IS NULL)) THEN "Mobile Only"
 END AS E_FMC_Status, f.*,m.*,
FROM Fixed_Base f FULL OUTER JOIN Mobile_Base m 
ON safe_cast(Fixed_Account as string)=B_Contrato AND Fixed_Month=Mobile_Month
)

--,CustomerBase_FMCFlags AS(
 
 SELECT t.*,
 CASE 
 WHEN (B_FMC_Status = "Fixed Only" OR (E_FMC_Status = "FMC soft")  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL)) AND B_MIX = "1P" THEN "Fixed 1P"
 WHEN (B_FMC_Status = "Fixed Only" OR (E_FMC_Status = "FMC soft")  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL)) AND B_MIX = "2P" THEN "Fixed 2P"
 WHEN (B_FMC_Status = "Fixed Only" OR (E_FMC_Status = "FMC soft")  AND (Mobile_ActiveBOM = 0 OR MOBILE_ACTIVEBOM IS NULL)) AND B_MIX = "3P" THEN "Fixed 3P"
 WHEN (B_FMC_Status = "FMC soft") AND (ActiveBOM = 0 OR ActiveBOM is null) then "Mobile Only"
 WHEN B_FMC_Status = "FMC soft" OR  B_FMC_Status = "Mobile Only" THEN B_FMC_Status
 END AS B_FMCType,
--Pendiente End of Month--
 FROM FullCustomerBase t


/*SELECT DISTINCT MONTH, E_FMC_Status, COUNT(*)
FROM FullCustomerBase
WHERE MONTH = "2022-02-01"
GROUP BY Month, E_FMC_Status*/
