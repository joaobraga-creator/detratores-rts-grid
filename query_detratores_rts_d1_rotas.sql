WITH datas AS (
  SELECT DATE_SUB(MAX(data), INTERVAL 1 DAY) AS d1
  FROM `meli-bi-data.SBOX_MLBPLACES.DETRATORES_RTS_BASE`
)
SELECT
  CAST(data AS STRING) AS data,
  facility,
  rota,
  nodo,
  nome_nodo,
  placa,
  transportadora,
  regional,
  tipo,
  COUNT(DISTINCT shipment_id) AS ruteados,
  COUNT(DISTINCT CASE WHEN flg_coletado = 1 THEN shipment_id END) AS coletados,
  COUNT(DISTINCT CASE WHEN flg_online = 1 AND flg_offline = 0 THEN shipment_id END) AS online,
  COUNT(DISTINCT CASE WHEN flg_offline = 1 THEN shipment_id END) AS offline,
  COUNT(DISTINCT shipment_id) - COUNT(DISTINCT CASE WHEN flg_coletado = 1 THEN shipment_id END) AS nao_coletados,
  SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN flg_coletado = 1 THEN shipment_id END), COUNT(DISTINCT shipment_id)) AS pct_coleta,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', MAX(refreshed_at), 'America/Sao_Paulo') AS atualizado_em
FROM `meli-bi-data.SBOX_MLBPLACES.DETRATORES_RTS_BASE`, datas
WHERE data = datas.d1
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY nao_coletados DESC, ruteados DESC;
