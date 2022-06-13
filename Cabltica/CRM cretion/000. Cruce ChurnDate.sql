WITH

DNA AS(
SELECT * FROM `dev-fortress-335113.DNA_Data.Final_DNA` 
)

,ServiceOrders AS(
  SELECT * FROM `dev-fortress-335113.cabletica_ontological_prod_final.order_line_item`
WHERE
  ORDER_TYPE = "DESINSTALACION" 
  AND (ORDER_STATUS <> "CANCELADA" OR ORDER_STATUS <> "ANULADA")
 AND ORDER_START_DATE IS NOT NULL
)

,CRM_With_Churn AS(
SELECT f.*,ORDER_START_DATE AS ChurnDate
FROM DNA f LEFT JOIN ServiceOrders a ON  date_trunc(ORDER_START_DATE,Month)=date_trunc(load_dt,Month) AND act_acct_name=safe_cast(a.account_name as string)
)

SELECT distinct * FROM CRM_With_Churn
WHERE ChurnDate IS NOT NULL
