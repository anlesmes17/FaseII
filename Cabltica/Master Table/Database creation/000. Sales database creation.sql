--CREATE OR REPLACE TABLE
--`gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220510-Altas_ene_2021_jun_2022` AS


WITH

Historic_sales as(
  select * From `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220510-Altas_ene_2021_abr_2022`
)

,May_June_sales as(
SELECT 
safe_cast(Formato_Fecha as string),
safe_cast(Contrato as string),
N__mero_Activo,
Tipo_Identificaci__n,
safe_cast(N__mero_Tel__fono as string),
N__mero_Tel__fono_Celular,
Cliente,
Identificaci__n,
Correo_Electronico,
Divisi__n,
Subcanal__Venta,
Categor__a_Canal,
Vendedor,
N__mero_Orden,
SubRegi__n,
Tipo_Cliente,
Provincia,
Cant__n,
Distrito,
SubProducto,
Activo,
SubCategor__a,
Tipo_Movimiento,
Motivo,
Estado_Activo,
Tipo_Alta,
 FROM `gcp-bia-tmps-vtr-dev-01.gcp_temp_cr_dev_01.20220715_Altas_Mayo_Junio_2022`
)

--,New_sales as(
  select * From Historic_sales
  UNION ALL
  select * From May_June_sales
--)
