import fs from 'node:fs/promises';

const DOC_ID        = process.env.GRID_DOC_ID    || '01KVR65T6TXSTK9FQERHAC8VET';
const GRID_TOKEN    = process.env.GRID_TOKEN      || 'grid_sk_01KWHGJWJRX8CYW64P6DZFSKQD';
const GRID_BASE_URL = process.env.GRID_BASE_URL   || 'https://grid.melioffice.com';

const datasets = [
  ['detratores_rts_meta',          'output/grid-datasets/detratores_rts_meta.json'],
  ['detratores_rts_resumo_diario', 'output/grid-datasets/detratores_rts_resumo_diario.json'],
  ['detratores_rts_resumo_nodo',   'output/grid-datasets/detratores_rts_resumo_nodo.json'],
  ['detratores_rts_resumo_placa',  'output/grid-datasets/detratores_rts_resumo_placa.json'],
  ['detratores_rts_rotas_30d',     'output/grid-datasets/detratores_rts_rotas_30d.json'],
  ['detratores_rts_d0_rotas',      'output/grid-datasets/detratores_rts_d0_rotas.json'],
  ['detratores_rts_d1_rotas',      'output/grid-datasets/detratores_rts_d1_rotas.json'],
  ['detratores_rts_d1_detalhe',    'output/grid-datasets/detratores_rts_d1_detalhe.json'],
  ['detratores_rts_pacotes_d0_d1', 'output/grid-datasets/detratores_rts_pacotes_d0_d1.json'],
];

const authHeaders = {
  Authorization: `Bearer ${GRID_TOKEN}`,
  'Content-Type': 'application/json',
};

async function readError(response) {
  const text = await response.text().catch(() => '');
  return `${response.status} ${response.statusText}${text ? `: ${text}` : ''}`;
}

async function ensureDataset(name) {
  const response = await fetch(`${GRID_BASE_URL}/api/v1/documents/${DOC_ID}/datasets/${name}`, {
    method: 'POST',
    headers: authHeaders,
    body: JSON.stringify({
      source_type: 'external_push',
      refresh_mode: 'external',
      format: 'json_rows',
      source: 'automation',
    }),
  });
  if (response.ok || response.status === 409) return;
  throw new Error(`Falha criando dataset ${name}: ${await readError(response)}`);
}

async function publishDataset(name, filePath) {
  await ensureDataset(name);

  const rows = JSON.parse(await fs.readFile(filePath, 'utf8'));
  const body = JSON.stringify({ rows: Array.isArray(rows) ? rows : [rows] });

  for (let attempt = 1; attempt <= 5; attempt += 1) {
    const response = await fetch(`${GRID_BASE_URL}/api/v1/documents/${DOC_ID}/data/${name}`, {
      method: 'PUT',
      headers: authHeaders,
      body,
    });

    if (response.ok) return;

    const detail = await response.text().catch(() => '');

    if (response.status === 503 && attempt < 5) {
      const delay = 3000 + Math.random() * 3000;
      console.warn(`503 em ${name}; aguardando ${Math.round(delay)}ms (${attempt + 1}/5)...`);
      await new Promise(r => setTimeout(r, delay));
      continue;
    }

    if (response.status === 404 && attempt < 5) {
      console.warn(`404 em ${name}; recriando dataset (${attempt + 1}/5)...`);
      await ensureDataset(name);
      await new Promise(r => setTimeout(r, 1000));
      continue;
    }

    throw new Error(`Falha publicando ${name}: ${response.status} ${response.statusText}${detail ? `: ${detail}` : ''}`);
  }

  throw new Error(`Falha publicando ${name}: tentativas esgotadas`);
}

for (const [name, filePath] of datasets) {
  const stat = await fs.stat(filePath);
  console.log(`Publicando ${name} (${stat.size} bytes)...`);
  await publishDataset(name, filePath);
  console.log(`OK ${name}`);
}

console.log('Todos os datasets foram publicados.');
