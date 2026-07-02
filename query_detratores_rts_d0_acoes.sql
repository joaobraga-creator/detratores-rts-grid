WITH params AS (
  SELECT MAX(data) AS data_ref
  FROM `meli-bi-data.SBOX_MLBPLACES.DETRATORES_RTS_BASE`
),
base AS (
  SELECT
    b.data,
    b.shipment_id,
    b.rota,
    b.nodo,
    b.nome_nodo,
    b.facility,
    b.regional,
    b.tipo,
    b.flg_coletado,
    b.flg_online,
    b.flg_offline,
    b.flg_nao_coletado
  FROM `meli-bi-data.SBOX_MLBPLACES.DETRATORES_RTS_BASE` b
  CROSS JOIN params p
  WHERE b.data = p.data_ref
    AND b.nodo IS NOT NULL
),
rotas_por_nodo AS (
  SELECT
    nodo,
    rota,
    COUNT(DISTINCT shipment_id) AS ruteados_rota,
    COUNT(DISTINCT CASE WHEN flg_coletado = 1 THEN shipment_id END) AS coletados_rota
  FROM base
  GROUP BY 1, 2
),
rotas_sem_coleta AS (
  SELECT
    nodo,
    STRING_AGG(CAST(rota AS STRING), ', ' ORDER BY (ruteados_rota - coletados_rota) DESC LIMIT 10) AS rotas_sem_coleta
  FROM rotas_por_nodo
  WHERE ruteados_rota > coletados_rota
  GROUP BY 1
),
agg AS (
  SELECT
    CAST(data AS STRING) AS data,
    nodo,
    ANY_VALUE(nome_nodo) AS nome_nodo,
    ANY_VALUE(facility) AS facility,
    ANY_VALUE(regional) AS regional,
    ANY_VALUE(tipo) AS tipo,
    COUNT(DISTINCT shipment_id) AS ruteados,
    COUNT(DISTINCT CASE WHEN flg_coletado = 1 THEN shipment_id END) AS coletados,
    COUNT(DISTINCT CASE WHEN flg_online = 1 AND flg_offline = 0 THEN shipment_id END) AS online,
    COUNT(DISTINCT CASE WHEN flg_offline = 1 THEN shipment_id END) AS offline,
    COUNT(DISTINCT shipment_id) - COUNT(DISTINCT CASE WHEN flg_coletado = 1 THEN shipment_id END) AS nao_coletados
  FROM base
  GROUP BY 1, 2
),
ranked AS (
  SELECT
    a.*,
    COALESCE(r.rotas_sem_coleta, 'nenhuma') AS rotas_sem_coleta,
    SAFE_DIVIDE(a.coletados, a.ruteados) AS pct_coleta,
    SAFE_DIVIDE(a.online, a.ruteados) AS pct_online,
    SAFE_DIVIDE(a.offline, a.ruteados) AS pct_offline
  FROM agg a
  LEFT JOIN rotas_sem_coleta r USING (nodo)
)
SELECT
  data,
  'DETRATOR RTS' AS kpi,
  nodo AS place_id,
  nodo,
  nome_nodo,
  facility,
  regional,
  tipo,
  CASE
    WHEN pct_coleta < 0.70 OR nao_coletados >= 50 THEN 'ALTA'
    WHEN pct_coleta < 0.85 OR nao_coletados >= 20 THEN 'MEDIA'
    WHEN nao_coletados = 0 THEN 'OK'
    ELSE 'BAIXA'
  END AS prioridade,
  CAST(CURRENT_DATE('America/Sao_Paulo') AS STRING) AS prazo,
  rotas_sem_coleta,
  CONCAT('Rotas sem coleta: ', rotas_sem_coleta) AS justificativa,
  CONCAT(
    CAST(nao_coletados AS STRING), ' pacotes nao coletados de ',
    CAST(ruteados AS STRING), ' roteados (',
    CAST(ROUND(pct_coleta * 100, 1) AS STRING), '% coleta). ',
    'Online: ', CAST(online AS STRING), ' (', CAST(ROUND(pct_online * 100, 1) AS STRING), '%) | ',
    'Offline: ', CAST(offline AS STRING), ' (', CAST(ROUND(pct_offline * 100, 1) AS STRING), '%). ',
    'Nodo: ', COALESCE(nome_nodo, nodo), ' - SVC: ', facility
  ) AS comentario,
  ruteados,
  coletados,
  online,
  offline,
  nao_coletados,
  pct_coleta,
  pct_online,
  pct_offline,
  '' AS crm_id,
  '' AS crm_desc,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS atualizado_em
FROM ranked
ORDER BY
  CASE
    WHEN pct_coleta < 0.70 OR nao_coletados >= 50 THEN 1
    WHEN pct_coleta < 0.85 OR nao_coletados >= 20 THEN 2
    WHEN nao_coletados = 0 THEN 4
    ELSE 3
  END,
  nao_coletados DESC,
  ruteados DESC;
