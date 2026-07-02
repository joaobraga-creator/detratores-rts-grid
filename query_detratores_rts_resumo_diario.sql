SELECT
  CAST(data AS STRING) AS data,
  facility,
  regional,
  tipo,
  COUNT(DISTINCT shipment_id) AS ruteados,
  COUNT(DISTINCT CASE WHEN flg_coletado = 1 THEN shipment_id END) AS coletados,
  COUNT(DISTINCT CASE WHEN flg_online = 1 AND flg_offline = 0 THEN shipment_id END) AS online,
  COUNT(DISTINCT CASE WHEN flg_offline = 1 THEN shipment_id END) AS offline,
  COUNT(DISTINCT shipment_id) - COUNT(DISTINCT CASE WHEN flg_coletado = 1 THEN shipment_id END) AS nao_coletados,
  SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN flg_coletado = 1 THEN shipment_id END), COUNT(DISTINCT shipment_id)) AS pct_coleta,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', MAX(refreshed_at), 'America/Sao_Paulo') AS atualizado_em
FROM `meli-bi-data.SBOX_MLBPLACES.DETRATORES_RTS_BASE`
GROUP BY 1, 2, 3, 4
ORDER BY data DESC, nao_coletados DESC;
