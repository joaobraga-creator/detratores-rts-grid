CREATE OR REPLACE TABLE `meli-bi-data.SBOX_MLBPLACES.DETRATORES_RTS_BASE`
PARTITION BY data
CLUSTER BY regional, facility, transportadora, nodo
OPTIONS (
  description = 'Base deduplicada por pacote roteado para dashboard Detratores de RTS.'
) AS
WITH last_mile AS (
  SELECT
    CAST(SHP_LG_ROUTE_ID AS STRING) AS rota,
    UPPER(TRIM(SHP_LG_FACILITY_ID)) AS facility_lm,
    UPPER(TRIM(REGION_OTR)) AS regional_lm,
    UPPER(TRIM(SHP_COMPANY_NAME)) AS transportadora_lm,
    UPPER(TRIM(SHP_LG_VEHICLE_PLATE_ID)) AS placa_lm,
    SHP_LG_ROUTE_INIT_DATE,
    SHP_LG_ROUTE_END_DATE,
    AUD_UPD_DTTM,
    AUD_INS_DTTM
  FROM `meli-bi-data.WHOWNER.BT_MLB_LAST_MILE`
  WHERE SHP_LG_ROUTE_ID IS NOT NULL
    AND DATE(COALESCE(SHP_LG_ROUTE_INIT_DATE, AUD_UPD_DTTM, AUD_INS_DTTM))
      >= DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 45 DAY)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CAST(SHP_LG_ROUTE_ID AS STRING)
    ORDER BY COALESCE(SHP_LG_ROUTE_END_DATE, SHP_LG_ROUTE_INIT_DATE, AUD_UPD_DTTM, AUD_INS_DTTM) DESC
  ) = 1
),
carteira_svc AS (
  SELECT
    UPPER(TRIM(PLC_PLACE_SVC)) AS facility,
    ANY_VALUE(UPPER(TRIM(PLC_PLACE_SUBREGIONAL))) AS regional_carteira,
    ANY_VALUE(UPPER(TRIM(PLC_PLACE_TIPO))) AS tipo_svc
  FROM `meli-bi-data.WHOWNER.BT_CARTEIRA_MLB`
  WHERE PLC_PLACE_SVC IS NOT NULL
  GROUP BY 1
),
carteira_nodo AS (
  SELECT
    UPPER(TRIM(PLC_PLACE_FACILITY)) AS nodo,
    ANY_VALUE(UPPER(TRIM(PLC_PLACE_SUBREGIONAL))) AS regional_nodo,
    ANY_VALUE(UPPER(TRIM(PLC_PLACE_TIPO))) AS tipo_nodo
  FROM `meli-bi-data.WHOWNER.BT_CARTEIRA_MLB`
  WHERE PLC_PLACE_FACILITY IS NOT NULL
  GROUP BY 1
),
pickup AS (
  SELECT
    DATE(s.SHP_LG_INIT_DT_TZ) AS data,
    CAST(s.SHP_SHIPMENT_ID AS STRING) AS shipment_id,
    UPPER(TRIM(s.SHP_LG_FACILITY_ID)) AS facility,
    CAST(s.SHP_LG_ROUTE_ID AS STRING) AS rota,
    UPPER(TRIM(s.SHP_NODE_ID)) AS nodo,
    UPPER(TRIM(s.SHP_NODE_ID_TYPE)) AS tipo_nodo_pickup,
    UPPER(TRIM(s.SHP_NODE_NAME)) AS nome_nodo_pickup,
    UPPER(TRIM(s.SHP_LG_VEHICLE_PLATE_ID)) AS placa_pickup,
    UPPER(TRIM(s.SHP_COMPANY_NAME)) AS transportadora_pickup,
    CAST(COALESCE(s.PUS_LM_IS_COLLECTED, 0) AS INT64) AS is_collected,
    CAST(COALESCE(s.PUS_LM_IS_COLLECTED_OFFLINE, 0) AS INT64) AS is_collected_offline,
    CAST(COALESCE(s.PUS_LM_IS_RUTEADO, 0) AS INT64) AS is_ruteado,
    s.AUD_UPD_DTTM,
    s.AUD_INS_DTTM
  FROM `meli-bi-data.WHOWNER.BT_SHP_SHIPMENTS_LAST_MILE_PICKUP` s
  WHERE s.SHP_SITE_ID = 'MLB'
    AND s.PUS_LM_IS_RUTEADO = 1
    AND s.SHP_SHIPMENT_ID IS NOT NULL
    AND s.SHP_LG_INIT_DT_TZ BETWEEN DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 30 DAY)
      AND CURRENT_DATE('America/Sao_Paulo')
),
dedup AS (
  SELECT
    p.*
  FROM pickup p
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY shipment_id
    ORDER BY
      data DESC,
      COALESCE(AUD_UPD_DTTM, AUD_INS_DTTM) DESC,
      is_collected DESC,
      is_collected_offline DESC
  ) = 1
)
SELECT
  d.data,
  d.shipment_id,
  d.facility,
  COALESCE(csvc.regional_carteira, cnodo.regional_nodo, lm.regional_lm, 'SEM_REGIONAL') AS regional,
  COALESCE(NULLIF(d.tipo_nodo_pickup, ''), NULLIF(cnodo.tipo_nodo, ''), NULLIF(csvc.tipo_svc, ''), 'SEM_TIPO') AS tipo,
  d.rota,
  d.nodo,
  COALESCE(
    NULLIF(
      REGEXP_REPLACE(
        COALESCE(NULLIF(d.nome_nodo_pickup, ''), d.nodo, 'SEM_NODO'),
        r'^AG.*MERCADO[[:space:]]+LIVRE[[:space:]]*-?[[:space:]]*',
        ''
      ),
      ''
    ),
    'SEM_NODO'
  ) AS nome_nodo,
  COALESCE(NULLIF(d.placa_pickup, ''), NULLIF(lm.placa_lm, ''), 'SEM_PLACA') AS placa,
  COALESCE(NULLIF(d.transportadora_pickup, ''), NULLIF(lm.transportadora_lm, ''), 'SEM_TRANSPORTADORA') AS transportadora,
  d.is_collected,
  d.is_collected_offline,
  COALESCE(d.AUD_UPD_DTTM, d.AUD_INS_DTTM) AS source_updated_at,
  CASE WHEN d.is_collected = 1 AND d.is_collected_offline = 0 THEN 1 ELSE 0 END AS flg_online,
  CASE WHEN d.is_collected = 1 AND d.is_collected_offline = 1 THEN 1 ELSE 0 END AS flg_offline,
  CASE
    WHEN d.is_collected = 1 THEN 1
    ELSE 0
  END AS flg_coletado,
  CASE
    WHEN d.is_collected = 1 THEN 0
    ELSE 1
  END AS flg_nao_coletado,
  CURRENT_TIMESTAMP() AS refreshed_at
FROM dedup d
LEFT JOIN last_mile lm
  ON lm.rota = d.rota
LEFT JOIN carteira_svc csvc
  ON csvc.facility = d.facility
LEFT JOIN carteira_nodo cnodo
  ON cnodo.nodo = d.nodo;

SELECT
  'ok' AS status,
  CURRENT_TIMESTAMP() AS executed_at;
