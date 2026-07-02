SELECT
  CAST(data AS STRING) AS data,
  shipment_id,
  regional,
  facility,
  tipo,
  rota,
  nodo,
  nome_nodo,
  transportadora,
  placa,
  is_collected,
  is_collected_offline,
  flg_online AS online,
  flg_offline AS offline,
  flg_coletado AS coletado,
  flg_nao_coletado AS nao_coletado,
  FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', source_updated_at) AS source_updated_at,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', refreshed_at, 'America/Sao_Paulo') AS refreshed_at
FROM `meli-bi-data.SBOX_MLBPLACES.DETRATORES_RTS_BASE`
WHERE data BETWEEN DATE_SUB(
    (SELECT MAX(data) FROM `meli-bi-data.SBOX_MLBPLACES.DETRATORES_RTS_BASE`),
    INTERVAL 30 DAY
  )
  AND (SELECT MAX(data) FROM `meli-bi-data.SBOX_MLBPLACES.DETRATORES_RTS_BASE`)
ORDER BY data DESC, regional, facility, rota, shipment_id;
