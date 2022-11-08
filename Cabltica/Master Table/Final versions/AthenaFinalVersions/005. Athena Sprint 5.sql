WITH

FmcTable AS (
SELECT DISTINCT * FROM "lla_cco_int_san"."cr_fmc_table"
)

------------------------------------ One time and repeated callers --------------------------

,Interactions_Fields as(
  select distinct *,date_trunc('Month',interaction_start_time)  as month,account_id AS ContratoInteractions
  From "interactions_cabletica"
)

,Last_Interaction as (
  select account_id AS last_account,
  first_value(Fecha_Apertura) over(partition by account_id,date_trunc('Month',interaction_start_time) order by interaction_start_time desc) as last_interaction_date
  From "interactions_cabletica"
  where
        AND interaction_status <> "ANULADA"
        AND TIPO <> "GESTION COBRO"
        AND MOTIVO <> "LLAMADA  CONSULTA DESINSTALACION"
)
