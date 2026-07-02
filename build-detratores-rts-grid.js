import fs from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { gzipSync, gunzipSync } from 'node:zlib';
import { BigQuery } from '@google-cloud/bigquery';
import AdmZip from 'adm-zip';

const DOC_ID = process.env.GRID_DOC_ID || '01KVR65T6TXSTK9FQERHAC8VET';
const PROJECT_ID = process.env.BQ_PROJECT_ID || 'meli-bi-data';
const LOCATION = process.env.BQ_LOCATION || 'US';
const LOGO_PATH = process.env.ML_LOGO_PATH || '';
const LOGO_URL = 'https://http2.mlstatic.com/frontend-assets/ml-web-navigation/ui-navigation/6.6.92/mercadolibre/logo__large_plus.png';

function loadServiceAccountFromBase64(value) {
  const json = Buffer.from(value.replace(/\s/g, ''), 'base64')
    .toString('utf8')
    .replace(/^\uFEFF/, '');
  return JSON.parse(json);
}

function createBigQueryClient() {
  if (process.env.GOOGLE_CREDENTIALS_BASE64) {
    return new BigQuery({
      projectId: PROJECT_ID,
      credentials: loadServiceAccountFromBase64(process.env.GOOGLE_CREDENTIALS_BASE64)
    });
  }

  if (process.env.GOOGLE_CREDENTIALS_BASE64_FILE) {
    return new BigQuery({
      projectId: PROJECT_ID,
      credentials: loadServiceAccountFromBase64(readFileSync(process.env.GOOGLE_CREDENTIALS_BASE64_FILE, 'utf8'))
    });
  }

  return new BigQuery({ projectId: PROJECT_ID });
}

const bigquery = createBigQueryClient();

const queries = {
  resumo_diario: 'query_detratores_rts_resumo_diario.sql',
  resumo_nodo: 'query_detratores_rts_resumo_nodo.sql',
  resumo_placa: 'query_detratores_rts_resumo_placa.sql',
  detratores_rotas: 'query_detratores_rts_rotas_top.sql',
  d0_rotas: 'query_detratores_rts_d0_rotas.sql',
  d1_rotas: 'query_detratores_rts_d1_rotas.sql',
  d1_acoes: 'query_detratores_rts_d1_acoes.sql',
  pacotes: 'query_detratores_rts_pacotes.sql'
};

function normalizeBigQueryValue(value) {
  if (value == null) return value;
  if (Array.isArray(value)) return value.map(normalizeBigQueryValue);
  if (typeof value === 'object') {
    if ('value' in value && Object.keys(value).length === 1) return value.value;
    if (value.constructor?.name === 'BigQueryDate') return value.value;
    if (value.constructor?.name === 'BigQueryTimestamp') return value.value;
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, normalizeBigQueryValue(v)]));
  }
  return value;
}

async function runBq(query) {
  const [job] = await bigquery.createQueryJob({
    query,
    location: LOCATION,
    useLegacySql: false
  });
  const rows = [];
  let options = { maxResults: 25000, autoPaginate: false };
  let nextLogAt = 100000;

  while (true) {
    const [pageRows, nextQuery] = await job.getQueryResults(options);
    for (const row of pageRows) rows.push(normalizeBigQueryValue(row));
    if (rows.length >= nextLogAt) {
      console.log(`  ${rows.length} linhas carregadas...`);
      nextLogAt += 100000;
    }
    if (!nextQuery) return rows;
    options = { ...nextQuery, maxResults: 25000, autoPaginate: false };
  }
}

function esc(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function jsonForHtml(value) {
  return JSON.stringify(value).replaceAll('<', '\\u003c');
}

function num(value) {
  return Number(value || 0);
}

function compactDatasetsForLiveDataset(datasets) {
  const compact = { ...datasets };
  if (Array.isArray(compact.pacotes)) {
    const dates = [...new Set(compact.pacotes.map(row => row.data).filter(Boolean))]
      .sort()
      .slice(-2);
    compact.pacotes = compact.pacotes.filter(row => dates.includes(row.data));
  }
  return compact;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function publishGrid(html) {
  await fs.mkdir('output/detratores-rts-site', { recursive: true });
  await fs.writeFile('output/detratores-rts-site/index.html', html, 'utf8');
  await fs.rm('output/detratores-rts-site.zip', { force: true });
  const zip = new AdmZip();
  zip.addFile('index.html', Buffer.from(html, 'utf8'));
  const bytes = zip.toBuffer();
  await fs.writeFile('output/detratores-rts-site.zip', bytes);
  const start = await fetch('https://grid.melioffice.com/api/v1/documents/upload-url', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      filename: 'dash_rts.zip',
      existing_doc_id: DOC_ID,
      content_type: 'application/zip'
    })
  });
  if (!start.ok) throw new Error(`upload-url ${start.status}: ${await start.text()}`);
  const meta = await start.json();
  const put = await fetch(meta.upload_url, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/zip', 'Content-Length': String(bytes.length) },
    body: bytes
  });
  if (!put.ok) throw new Error(`put ${put.status}: ${await put.text()}`);
  const confirm = await fetch(`https://grid.melioffice.com/api/v1/documents/${meta.doc_id}/confirm`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ file_size: bytes.length, idempotency_key: `detratores-rts-${Date.now()}` })
  });
  if (!confirm.ok) throw new Error(`confirm ${confirm.status}: ${await confirm.text()}`);
  return await confirm.json();
}

async function publishGridWithRetry(html, attempts = 5) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      if (attempt > 1) console.log(`Tentando publicar novamente (${attempt}/${attempts})...`);
      return await publishGrid(html);
    } catch (error) {
      lastError = error;
      console.log(`Falha ao publicar (${attempt}/${attempts}): ${error.message}`);
      if (attempt < attempts) await sleep(10000);
    }
  }
  throw lastError;
}

async function loadExistingPayload() {
  const html = await fs.readFile('output/detratores-rts-grid.html', 'utf8');
  const match = html.match(/const PAYLOAD='([^']+)'/);
  if (!match) throw new Error('PAYLOAD antigo nao encontrado em output/detratores-rts-grid.html');
  return JSON.parse(gunzipSync(Buffer.from(match[1], 'base64')).toString('utf8'));
}

async function loadLogoSrc() {
  try {
    const logoBase64 = (await fs.readFile(LOGO_PATH)).toString('base64');
    return `data:image/png;base64,${logoBase64}`;
  } catch {
    return LOGO_URL;
  }
}

function buildHtml({ datasets, logoSrc, validation, queryTexts }) {
  const payload = gzipSync(Buffer.from(JSON.stringify({ datasets, validation, queryTexts }), 'utf8'), { level: 9 }).toString('base64');
  const latestUpdate =
    datasets.resumo_diario[0]?.atualizado_em ||
    new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' });
  const sourceUpdate = validation.fonte_atualizada_em || 'sem horario fonte';
  const baseUpdate = validation.atualizado_em || latestUpdate;
  const gridUpdate = new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' });
  const minDate = datasets.resumo_diario.reduce((m, r) => (!m || r.data < m ? r.data : m), '');
  const maxDate = datasets.resumo_diario.reduce((m, r) => (!m || r.data > m ? r.data : m), '');
  const todaySaoPaulo = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Sao_Paulo',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).format(new Date());
  const yesterday = new Date(`${todaySaoPaulo}T12:00:00Z`);
  yesterday.setUTCDate(yesterday.getUTCDate() - 1);
  const yesterdaySaoPaulo = yesterday.toISOString().slice(0, 10);

  return `<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Detratores de RTS</title><script src="/d/_assets/grid-sdk.js"></script><style>
:root{--ml-yellow:#ffe600;--ml-blue:#183a8c;--bg:#eef2f6;--surface:#fff;--surface2:#f7f9fc;--text:#061a3b;--muted:#60708a;--line:#d5deea;--green:#00a650;--blue:#3483fa;--purple:#8a63d2;--red:#e63946;--orange:#f5a400;--shadow:0 8px 18px rgba(10,31,68,.08)}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,Segoe UI,Roboto,Arial,sans-serif;font-size:12px;letter-spacing:0}button,input,select{font:inherit}.top{max-width:1480px;height:72px;margin:0 auto;background:var(--ml-yellow);display:flex;align-items:center;justify-content:space-between;padding:0 14px;border-bottom:1px solid #d6c300}.brand{display:flex;gap:12px;align-items:center}.logo{width:58px;height:42px;object-fit:contain}.title h1{margin:0;font-size:18px;line-height:1.1;font-weight:900}.title p{margin:6px 0 0;font-size:10px;font-weight:750;color:#1a2a48}.stamp{text-align:right;font-size:10px;font-weight:750;color:#1a2a48}.stamp b{font-size:12px;color:#061a3b}.nav{max-width:1480px;height:38px;margin:0 auto;background:#11162a;display:flex;align-items:center;padding:0 14px;gap:18px;border-bottom:1px solid #11162a}.nav button{height:38px;border:0;background:transparent;color:#e6ecfb;font-weight:850;padding:0;cursor:pointer;border-bottom:3px solid transparent;font-size:11px}.nav button.active{color:#fff;border-color:var(--ml-yellow)}.wrap{max-width:1480px;margin:0 auto;padding:14px 14px 24px}.filters{background:var(--surface);border:1px solid var(--line);border-radius:8px;padding:12px;display:grid;grid-template-columns:repeat(6,minmax(120px,1fr));gap:10px;align-items:start;box-shadow:var(--shadow)}.field{display:flex;flex-direction:column;gap:5px}.field label{font-size:9px;font-weight:850;color:#59677b;text-transform:none}select,input{height:30px;border:1px solid #c9d4e4;border-radius:5px;background:#fff;color:var(--text);padding:0 8px;min-width:0}.multi{position:relative}.multi-toggle{width:100%;height:30px;border:1px solid #c9d4e4;border-radius:5px;background:#fff;color:#061a3b;padding:0 24px 0 8px;text-align:left;cursor:pointer;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.multi-toggle:after{content:'v';position:absolute;right:8px;color:#061a3b;font-weight:900}.multi-menu{display:none;position:absolute;z-index:20;top:34px;left:0;right:0;max-height:300px;overflow:auto;background:#fff;border:1px solid #c9d4e4;border-radius:6px;box-shadow:0 10px 24px rgba(10,31,68,.16);padding:6px}.multi-search{position:sticky;top:0;z-index:1;width:100%;height:28px;margin:0 0 6px;background:#fff}.multi.open .multi-menu{display:block}.check{display:flex;align-items:center;gap:7px;padding:6px 5px;border-radius:4px;font-size:10px;font-weight:800;color:#061a3b;cursor:pointer}.check:hover{background:#eef5ff}.check input{width:13px;height:13px;margin:0}.check.all{border-bottom:1px solid #edf1f5;margin-bottom:4px;color:#183a8c}input[type="search"]{min-width:0}.clear,.table-export{height:30px;border:1px solid var(--blue);background:#fff;color:var(--blue);font-weight:800;border-radius:5px;padding:0 10px;cursor:pointer;font-size:10px}.clear{margin-top:14px}.clear:hover,.table-export:hover{background:#eef5ff}.table-export{float:right;height:24px;margin-left:8px}.hero{display:flex;justify-content:space-between;gap:12px;align-items:center;margin:12px 0 10px;background:#fff7bf;border:1px solid #e5ce35;border-radius:7px;padding:8px 10px;color:#3d3100;font-size:10px}.hero b{color:#061a3b}.cards{display:grid;grid-template-columns:repeat(7,1fr);gap:8px;margin:12px 0}.card{background:var(--surface);border:1px solid var(--line);border-radius:8px;padding:10px 11px;box-shadow:var(--shadow);border-top:3px solid var(--blue)}.card.green{border-top-color:var(--green)}.card.purple{border-top-color:var(--purple)}.card.red{border-top-color:var(--red)}.card.orange{border-top-color:var(--orange)}.num{font-size:20px;font-weight:900;line-height:1.05}.lbl{color:var(--muted);font-size:10px;margin-top:6px}.grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}.grid3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px}.panel{background:var(--surface);border:1px solid var(--line);border-radius:8px;overflow:hidden;box-shadow:var(--shadow)}.panel h2{margin:0;padding:11px 13px;border-bottom:1px solid var(--line);font-size:12px;font-weight:900}.pager{float:right;display:inline-flex;align-items:center;gap:6px;color:var(--muted);font-size:9px}.pager button{height:20px;border:1px solid var(--line);border-radius:4px;background:#fff;color:var(--text);font-weight:900;cursor:pointer}.rows{padding:10px 12px;max-height:330px;overflow:auto}.bar{display:grid;grid-template-columns:155px 1fr 96px;gap:9px;align-items:center;margin:7px 0;font-size:11px}.bar b{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.bar .right small{display:block;color:var(--muted);font-size:8px;font-weight:850;line-height:1.1}.track{height:8px;background:#e6edf6;border-radius:99px;overflow:hidden}.fill{height:100%;background:var(--blue);border-radius:99px}.fill.green{background:var(--green)}.fill.orange{background:var(--orange)}.fill.red{background:var(--red)}.chart{height:280px;padding:10px 12px 14px;display:flex;align-items:end;gap:8px;border-top:1px solid #edf1f6;overflow:hidden}.day{flex:1;min-width:14px;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;gap:2px}.chart-label{height:14px;font-size:8px;font-weight:900;color:#1a2a48;line-height:1;text-align:center;white-space:nowrap}.chart-count{font-size:8px;color:#6d7b91;line-height:1;text-align:center;white-space:nowrap}.stack{width:100%;max-width:22px;display:flex;flex-direction:column;justify-content:flex-end;border-radius:2px 2px 0 0;overflow:hidden;background:#e6edf6}.seg.online{background:var(--blue)}.seg.offline{background:var(--purple)}.seg.fail{background:#dbe5f1}.day span{font-size:8px;color:#718197;writing-mode:vertical-rl;transform:rotate(180deg);height:48px;overflow:hidden}.table-wrap{overflow:auto;max-height:520px}.table{width:100%;border-collapse:separate;border-spacing:0;font-size:11px}.table th{position:sticky;top:0;z-index:2;background:#f6f8fb;color:#526174;text-align:left;border-bottom:1px solid var(--line);padding:8px 9px;font-weight:900;white-space:nowrap}.table td{border-bottom:1px solid #edf1f5;padding:8px 9px;vertical-align:middle;color:#061a3b;line-height:1.25}.table th:first-child,.table td:first-child{text-align:center;width:44px;min-width:44px}.table th.right,.table td.right{text-align:right;font-variant-numeric:tabular-nums}.table td b{display:inline-block;max-width:240px;white-space:normal;overflow-wrap:anywhere}.table .perf{min-width:150px;width:150px}.rank{display:inline-grid;place-items:center;width:19px;height:19px;border-radius:50%;background:#eef3ff;color:#1f4aae;font-weight:900}.rank.top{background:#e63946;color:#fff}.badge{display:inline-block;border-radius:999px;padding:3px 7px;font-weight:900;font-size:10px}.bad{background:#ffe9e9;color:#c62828}.ok{background:#e7f8ef;color:#008744}.mid{background:#fff5d6;color:#8a5d00}.right{text-align:right}.status{float:right;color:#6c7890;font-size:10px}.hide{display:none}::-webkit-scrollbar{width:8px;height:8px}::-webkit-scrollbar-thumb{background:#a9b6c8;border-radius:999px}::-webkit-scrollbar-track{background:#edf1f6}@media(max-width:1100px){.top,.nav,.wrap{max-width:none}.cards,.grid2,.grid3{grid-template-columns:1fr}.top{height:auto;align-items:flex-start;flex-direction:column;padding:14px}.stamp{text-align:left}.filters{grid-template-columns:1fr}.bar{grid-template-columns:120px 1fr 86px}.field,input[type="search"],select{width:100%;min-width:100%}}
</style></head><body><header class="top"><div class="brand"><img class="logo" src="${esc(logoSrc)}" alt="Mercado Livre"><div class="title"><h1>Detratores de RTS</h1><p>Abertura operacional por SVC, nodo, placa, transportadora e rota</p></div></div><div class="stamp">Tabela fonte atualizada<br><b>${esc(sourceUpdate)}</b><br><span>Base painel: ${esc(baseUpdate)}</span></div></header><nav class="nav"><button class="active" data-tab="svc">Por SVC</button><button data-tab="nodo">Por Nodo</button><button data-tab="placa">Por Placa</button><button data-tab="rota">Por Rota</button><button data-tab="d0">D0</button><button data-tab="d1">D-1</button><button data-tab="d1acoes">D-1 Detalhe</button><button data-tab="poc">POC Dedicado</button><button data-tab="fonte">Fonte</button></nav><main class="wrap"><section class="filters"><div class="field"><label>De</label><input id="de" type="date" value="${esc(yesterdaySaoPaulo)}"></div><div class="field"><label>Ate</label><input id="ate" type="date" value="${esc(todaySaoPaulo)}"></div><div class="field"><label>Regional</label><div class="multi" id="regionalBox"><button type="button" id="regionalBtn" class="multi-toggle">Todos</button><input id="regional" type="hidden"><div id="regionalMenu" class="multi-menu"></div></div></div><div class="field"><label>SVC</label><div class="multi" id="facilityBox"><button type="button" id="facilityBtn" class="multi-toggle">Todos</button><input id="facility" type="hidden"><div id="facilityMenu" class="multi-menu"></div></div></div><div class="field"><label>Tipo coleta</label><div class="multi" id="tipoBox"><button type="button" id="tipoBtn" class="multi-toggle">Todos</button><input id="tipo" type="hidden"><div id="tipoMenu" class="multi-menu"></div></div></div><div class="field"><label>Transportadora</label><div class="multi" id="transportadoraBox"><button type="button" id="transportadoraBtn" class="multi-toggle">Todos</button><input id="transportadora" type="hidden"><div id="transportadoraMenu" class="multi-menu"></div></div></div><div class="field"><label>Nodo</label><div class="multi" id="nodoBox"><button type="button" id="nodoBtn" class="multi-toggle">Todos</button><input id="nodo" type="hidden"><div id="nodoMenu" class="multi-menu"></div></div></div><div class="field"><label>Busca livre</label><input id="busca" type="search" placeholder="SVC, nodo, placa, rota..."></div><button id="limpar" class="clear">Limpar</button><button id="exportPacotes" class="clear">CSV pacotes filtrados</button></section><section class="hero"><div><b>Filtro inicial:</b> ${esc(yesterdaySaoPaulo)} ate ${esc(todaySaoPaulo)}. <b>D0 operacional:</b> ultima data carregada na tabela (${esc(maxDate)}). <b>Hora fonte:</b> ${esc(sourceUpdate)}. <b>Base painel:</b> ${esc(baseUpdate)}. <b>Grid publicado:</b> ${esc(gridUpdate)}.</div></section><section id="view"></section></main><script>
const PAYLOAD='${payload}';
const DEFAULT_DE='${esc(yesterdaySaoPaulo)}';
const DEFAULT_ATE='${esc(todaySaoPaulo)}';
const SHEETS_DATA_URL='${esc(process.env.SHEETS_DATA_URL || '')}';
const SHEETS_SPREADSHEET_ID='${esc(process.env.SHEETS_SPREADSHEET_ID || '1TbI4GRssQHoSG_9P5Lke_NW0ieRYyFRkJ7jZNYWHmGA')}';
const SHEETS_PUBLISHED_URL='${esc(process.env.SHEETS_PUBLISHED_URL || 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRRf4SvbOlP9R2dvZ4Y3ZmRHzjr5xtlDxLovTMQHjY83SuTYt64exvn7eG_ttgA8QbnDAzylZ-tzyrw/pubhtml')}';
const GRID_DOC_ID='${esc(DOC_ID)}';
const GRID_DATASET_NAME='${esc(process.env.GRID_DATASET_NAME || 'detratores_rts_payload')}';
const GRID_DATASET_PREFIX='${esc(process.env.GRID_DATASET_PREFIX || 'detratores_rts_')}';
const $=id=>document.getElementById(id);
const fmt=n=>new Intl.NumberFormat('pt-BR').format(Number(n||0));
const pct=n=>(Number(n||0)*100).toFixed(1).replace('.',',')+'%';
const esc=v=>String(v??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#39;');
let DATA={},tab='svc',EXPORTS={},BAR_PAGES={};
const DEDICADOS=['SSP6','SSP11','SMG6'];
function exportKey(name){return String(name).replace(/[^a-z0-9]+/gi,'_').toLowerCase()}
function exportButton(key){return '<button class="table-export" onclick="exportCsv(EXPORTS[&quot;'+key+'&quot;]||[],&quot;'+key+'&quot;)">CSV</button>'}
function exportCsv(rows,name){const keys=[...new Set((rows||[]).flatMap(r=>Object.keys(r).filter(k=>!k.startsWith('_'))))];const csv=[keys.join(';')].concat((rows||[]).map(r=>keys.map(k=>'"'+String(r[k]??'').replaceAll('"','""')+'"').join(';'))).join('\\r\\n');const a=document.createElement('a');a.href=URL.createObjectURL(new Blob(['\\ufeff'+csv],{type:'text/csv;charset=utf-8'}));a.download='detratores_rts_'+name+'.csv';a.click();URL.revokeObjectURL(a.href)}
function exportPacotes(){const rows=filtered(DATA.pacotes||[]);exportCsv(rows,'pacotes_filtrados')}
async function inflate(){const b=atob(PAYLOAD),bytes=new Uint8Array(b.length);for(let i=0;i<b.length;i++)bytes[i]=b.charCodeAt(i);const stream=new Response(bytes).body.pipeThrough(new DecompressionStream('gzip'));return JSON.parse(await new Response(stream).text())}
function configuredSheetsUrl(){return new URLSearchParams(location.search).get('dataUrl')||SHEETS_DATA_URL}
function configuredSheetId(){return new URLSearchParams(location.search).get('sheetId')||SHEETS_SPREADSHEET_ID}
function configuredPublishedUrl(){return new URLSearchParams(location.search).get('publishedUrl')||SHEETS_PUBLISHED_URL}
function loadSheetsJsonp(url){return new Promise((resolve,reject)=>{const cb='__detratoresSheets_'+Date.now(),sep=url.includes('?')?'&':'?',script=document.createElement('script'),timer=setTimeout(()=>{cleanup();reject(new Error('Timeout carregando dados do Sheets'))},45000);function cleanup(){clearTimeout(timer);delete window[cb];script.remove()}window[cb]=payload=>{cleanup();resolve(payload)};script.onerror=()=>{cleanup();reject(new Error('Falha carregando dados do Sheets'))};script.src=url+sep+'callback='+encodeURIComponent(cb)+'&_='+Date.now();document.head.appendChild(script)})}
function mapSheetsPayload(payload){const s=payload?.sheets||{},meta=s.meta||[],m=meta[0]||{};return{datasets:{meta,resumo_diario:s.resumo_diario||[],resumo_nodo:s.resumo_nodo||[],resumo_placa:s.resumo_placa||[],detratores_rotas:s.rotas_30d||[],d0_rotas:s.d0_rotas||[],d1_rotas:s.d1_rotas||[],d1_acoes:s.d1_detalhe||[],pacotes:s.pacotes_d0_d1||[]},validation:{fonte_atualizada_em:m.tabela_fonte_atualizada_em||'',atualizado_em:m.base_painel_atualizada_em||m.sheet_refresh_requested_at||''},queryTexts:{fonte:'Dados carregados diretamente das abas do Google Sheets.'}}}
function gvizValue(v){if(v==null)return'';if(v instanceof Date)return v.toISOString().slice(0,10);return v}
function loadGvizSheet(spreadsheetId,sheetName){return new Promise((resolve,reject)=>{const prior=window.google,script=document.createElement('script'),timer=setTimeout(()=>{cleanup();reject(new Error('Timeout carregando aba '+sheetName))},45000);function cleanup(){clearTimeout(timer);script.remove();window.google=prior}window.google={visualization:{Query:{setResponse:response=>{cleanup();if(response?.status==='error'){reject(new Error((response.errors||[]).map(e=>e.detailed_message||e.message).join('; ')||'Erro lendo aba '+sheetName));return}const table=response.table||{},headers=(table.cols||[]).map(c=>String(c.label||c.id||'').trim()),rows=(table.rows||[]).map(row=>{const item={};headers.forEach((h,i)=>{if(h)item[h]=gvizValue(row.c?.[i]?.v??row.c?.[i]?.f??'')});return item}).filter(r=>Object.values(r).some(v=>v!==''));resolve(rows)}}}};script.onerror=()=>{cleanup();reject(new Error('Falha carregando aba '+sheetName))};script.src='https://docs.google.com/spreadsheets/d/'+encodeURIComponent(spreadsheetId)+'/gviz/tq?sheet='+encodeURIComponent(sheetName)+'&headers=1&tqx=out:json&_='+Date.now();document.head.appendChild(script)})}
async function loadGoogleSheetPayload(spreadsheetId){const names={meta:'meta',resumo_diario:'resumo_diario',resumo_nodo:'resumo_nodo',resumo_placa:'resumo_placa',rotas_30d:'rotas_30d',d0_rotas:'d0_rotas',d1_rotas:'d1_rotas',d1_detalhe:'d1_detalhe',pacotes_d0_d1:'pacotes_d0_d1'},s={},errors=[];for(const [key,name]of Object.entries(names)){try{s[key]=await loadGvizSheet(spreadsheetId,name)}catch(err){errors.push(name+': '+(err?.message||String(err)));s[key]=[]}}const p=mapSheetsPayload({sheets:s});p.validation=p.validation||{};p.validation.sheets_load_error=errors.join(' | ');return p}
function publishedCsvUrl(base,sheetName){let clean=String(base||'').split('#')[0].split('?')[0];if(clean.endsWith('/pubhtml'))clean=clean.slice(0,-8)+'/pub';else if(!clean.endsWith('/pub')){if(clean.endsWith('/'))clean=clean.slice(0,-1);clean+='/pub'}return clean+'?single=true&output=csv&sheet='+encodeURIComponent(sheetName)+'&_='+Date.now()}
function parseCsv(text){const rows=[],cur=[];let cell='',q=false;for(let i=0;i<text.length;i++){const ch=text[i],nx=text[i+1],code=text.charCodeAt(i);if(q){if(ch=='"'&&nx=='"'){cell+='"';i++}else if(ch=='"'){q=false}else cell+=ch}else if(ch=='"'){q=true}else if(ch===','){cur.push(cell);cell=''}else if(code===10){cur.push(cell);rows.push(cur.splice(0));cell=''}else if(code!==13){cell+=ch}}cur.push(cell);if(cur.some(v=>v!==''))rows.push(cur);return rows}
async function loadPublishedCsvSheet(base,sheetName){const res=await fetch(publishedCsvUrl(base,sheetName),{credentials:'include',cache:'no-store'});if(!res.ok)throw new Error('Sheets publicado '+sheetName+' retornou HTTP '+res.status);const rows=parseCsv(await res.text());if(rows.length<2)return[];const headers=rows[0].map(h=>String(h||'').trim());return rows.slice(1).filter(r=>r.some(v=>v!=='')).map(r=>{const item={};headers.forEach((h,i)=>{if(h)item[h]=r[i]??''});return item})}
async function loadPublishedSheetPayload(base){const names={meta:'meta',resumo_diario:'resumo_diario',resumo_nodo:'resumo_nodo',resumo_placa:'resumo_placa',rotas_30d:'rotas_30d',d0_rotas:'d0_rotas',d1_rotas:'d1_rotas',d1_detalhe:'d1_detalhe',pacotes_d0_d1:'pacotes_d0_d1'},s={},errors=[];for(const [key,name]of Object.entries(names)){try{s[key]=await loadPublishedCsvSheet(base,name)}catch(err){errors.push(name+': '+(err?.message||String(err)));s[key]=[]}}const p=mapSheetsPayload({sheets:s});p.validation=p.validation||{};p.validation.sheets_load_error=errors.join(' | ');return p}
function withRequiredDatasets(p){const d=p.datasets||{};['meta','resumo_diario','resumo_nodo','resumo_placa','detratores_rotas','d0_rotas','d1_rotas','d1_acoes','pacotes'].forEach(k=>{if(!Array.isArray(d[k]))d[k]=[]});p.datasets=d;p.validation=p.validation||{};p.queryTexts=p.queryTexts||{};return p}
async function loadPayload(){const url=configuredSheetsUrl(),sheetId=configuredSheetId(),publishedUrl=configuredPublishedUrl(),errors=[];try{if(url){const p=mapSheetsPayload(await loadSheetsJsonp(url));if((p.datasets?.resumo_diario||[]).length)return p;errors.push('endpoint JSONP sem resumo_diario')}if(sheetId){const p=await loadGoogleSheetPayload(sheetId);if((p.datasets?.resumo_diario||[]).length)return p;errors.push(p.validation?.sheets_load_error||'planilha privada sem resumo_diario')}if(publishedUrl){const p=await loadPublishedSheetPayload(publishedUrl);if((p.datasets?.resumo_diario||[]).length)return p;errors.push(p.validation?.sheets_load_error||'planilha publicada sem resumo_diario')}}catch(err){errors.push(err?.message||String(err));console.error(err)}const fallback=await inflate();fallback.validation=fallback.validation||{};fallback.validation.sheets_load_error=errors.filter(Boolean).join(' | ');return fallback}
async function loadGridDatasetPayload(){if(!window.Grid)return null;Grid.configure({docId:GRID_DOC_ID});if(GRID_DATASET_NAME){try{const payload=await Grid.dataset(GRID_DATASET_NAME).load();const data=Array.isArray(payload)?payload[0]:payload;if(data?.datasets?.resumo_diario?.length)return data}catch(err){window.__GRID_DATASET_LOAD_ERROR=err?.message||String(err);console.error(err)}}const names={meta:'meta',resumo_diario:'resumo_diario',resumo_nodo:'resumo_nodo',resumo_placa:'resumo_placa',rotas_30d:'rotas_30d',d0_rotas:'d0_rotas',d1_rotas:'d1_rotas',d1_detalhe:'d1_detalhe',pacotes_d0_d1:'pacotes_d0_d1'},s={},errors=[];for(const [key,name]of Object.entries(names)){try{s[key]=await Grid.dataset(GRID_DATASET_PREFIX+name).load()}catch(err){errors.push(name+': '+(err?.message||String(err)));s[key]=[]}}const data=mapSheetsPayload({sheets:s});data.validation=data.validation||{};data.validation.grid_dataset_load_error=errors.join(' | ');if(data.datasets?.resumo_diario?.length)return data;return null}
async function loadData(){try{const data=await loadGridDatasetPayload();if(data)return data}catch(err){window.__GRID_DATASET_LOAD_ERROR=err?.message||String(err);console.error(err)}const data=await loadPayload();if(window.__GRID_DATASET_LOAD_ERROR){data.validation=data.validation||{};data.validation.grid_dataset_load_error=window.__GRID_DATASET_LOAD_ERROR}return data}
function syncMetadata(){const m=(DATA.meta||[])[0]||{},source=m.tabela_fonte_atualizada_em||DATA.validation?.fonte_atualizada_em||'sem horario fonte',base=m.base_painel_atualizada_em||DATA.validation?.atualizado_em||'sem horario base',d0=m.d0_operacional||'',d1=m.d1_operacional||'',sheet=m.sheet_refresh_requested_at||'',gridErr=DATA.validation?.grid_dataset_load_error||'',sheetErr=DATA.validation?.sheets_load_error||'',err=gridErr?('Dataset Grid: '+gridErr):(sheetErr?('Sheets: '+sheetErr):'');const stamp=document.querySelector('.stamp');if(stamp)stamp.innerHTML='Tabela fonte atualizada<br><b>'+esc(source)+'</b><br><span>Base painel: '+esc(base)+'</span>';const hero=document.querySelector('.hero div');if(hero)hero.innerHTML='<b>Filtro inicial:</b> '+esc($('de').value)+' ate '+esc($('ate').value)+'. <b>D0 operacional:</b> ultima data carregada na tabela ('+esc(d0||'n/d')+'). <b>Hora fonte:</b> '+esc(source)+'. <b>Base painel:</b> '+esc(base)+'. <b>Sheets atualizado:</b> '+esc(sheet||'n/d')+'.'+(err?' <b style="color:#c62828">Fonte dinamica indisponivel:</b> '+esc(err)+'. Usando fallback publicado.':'')}
function n(v){return Number(v||0)}
function sum(rows,key){return rows.reduce((s,r)=>s+n(r[key]),0)}
function canonicalRegional(value){const raw=String(value??'').trim();if(!raw)return raw;const k=raw.normalize('NFD').replace(/[\\u0300-\\u036f]/g,'').toUpperCase().replace(/[\\/_-]+/g,' ').replace(/\\s+/g,' ').trim();const map={'RJ ES':'RJ/ES','RJES':'RJ/ES','SP ZL':'SP ZL','ZL':'SP ZL','SP ZN':'SP ZN','ZN':'SP ZN','SP ZS':'SP ZS','ZS':'SP ZS','SP ZO':'SP ZO','ZO':'SP ZO','SPI CAMP':'SPI CAMP','SP CAMP':'SPI CAMP','SPICAMP':'SPI CAMP','SPI MAR':'SPI MAR','SP MAR':'SPI MAR','SPIMAR':'SPI MAR','SPI RAO':'SPI RAO','SP RAO':'SPI RAO','SPIRAO':'SPI RAO','NO CO':'NOCO','NOCO':'NOCO'};return map[k]||k}
function canonicalText(value){return String(value??'').trim().replace(/\\s+/g,' ').toUpperCase()}
function normalizeRecord(r){if(r.regional!=null)r.regional=canonicalRegional(r.regional);if(r.facility!=null)r.facility=canonicalText(r.facility);if(r.tipo!=null)r.tipo=canonicalText(r.tipo);if(r.transportadora!=null)r.transportadora=canonicalText(r.transportadora);if(r.nodo!=null)r.nodo=canonicalText(r.nodo);if(r.placa!=null)r.placa=canonicalText(r.placa);return r}
function normalizeDatasets(){for(const value of Object.values(DATA)){if(Array.isArray(value))value.forEach(normalizeRecord)}}
function currentDataset(){return tab==='nodo'?DATA.resumo_nodo:tab==='placa'?DATA.resumo_placa:tab==='rota'?DATA.detratores_rotas:tab==='d0'?DATA.d0_rotas:tab==='d1'?DATA.d1_rotas:tab==='d1acoes'?DATA.d1_acoes:DATA.resumo_diario}
function multiVals(id){return($(id).value||'').split('|').filter(Boolean)}
function inMulti(values,value){return!values.length||values.includes(String(value||''))}
function filters(){return{de:$('de').value,ate:$('ate').value,regional:multiVals('regional'),facility:multiVals('facility'),tipo:multiVals('tipo'),transportadora:multiVals('transportadora'),nodo:multiVals('nodo'),busca:$('busca').value.trim().toLowerCase()}}
function hit(r,f){if(f.de&&r.data<f.de)return false;if(f.ate&&r.data>f.ate)return false;if(!inMulti(f.regional,r.regional))return false;if(!inMulti(f.facility,r.facility))return false;if(!inMulti(f.tipo,r.tipo))return false;if(!inMulti(f.transportadora,r.transportadora))return false;if(!inMulti(f.nodo,r.nodo))return false;if(f.busca&&!Object.values(r).some(v=>String(v??'').toLowerCase().includes(f.busca)))return false;return true}
function filtered(rows=currentDataset()){const f=filters();return rows.filter(r=>hit(r,f))}
function uniq(rows,key){return [...new Set(rows.map(r=>r[key]).filter(Boolean))].sort((a,b)=>String(a).localeCompare(String(b),'pt-BR'))}
function setMulti(id,vals){$(id).value=vals.join('|');$(id+'Btn').textContent=!vals.length?'Todos':vals.length===1?vals[0]:vals.length+' selecionados';$(id+'Menu').querySelectorAll('input[data-value]').forEach(c=>{c.checked=vals.includes(c.dataset.value)});const all=$(id+'Menu').querySelector('input[data-all]');if(all)all.checked=!vals.length}
function toggleMulti(id,value,checked){let vals=multiVals(id);if(value===''){vals=[]}else if(checked&&!vals.includes(value)){vals.push(value)}else if(!checked){vals=vals.filter(v=>v!==value)}setMulti(id,vals);render()}
function filterMulti(input){const q=input.value.trim().toLowerCase();input.parentElement.querySelectorAll('label.check:not(.all)').forEach(l=>{l.style.display=!q||l.dataset.text.includes(q)?'flex':'none'})}
function fillMulti(id,values){const menu=$(id+'Menu'),keep=multiVals(id).filter(v=>values.includes(v));menu.innerHTML='<input class="multi-search" type="search" placeholder="Buscar..."><label class="check all"><input type="checkbox" data-all><span>Todos</span></label>'+values.map(v=>'<label class="check" data-text="'+esc(String(v).toLowerCase())+'"><input type="checkbox" data-value="'+esc(v)+'"><span>'+esc(v)+'</span></label>').join('');menu.querySelector('.multi-search').oninput=e=>filterMulti(e.target);menu.querySelector('input[data-all]').onchange=e=>toggleMulti(id,'',e.target.checked);menu.querySelectorAll('input[data-value]').forEach(c=>c.onchange=e=>toggleMulti(id,e.target.dataset.value,e.target.checked));setMulti(id,keep)}
function bindMulti(id){$(id+'Btn').onclick=e=>{e.stopPropagation();document.querySelectorAll('.multi.open').forEach(x=>{if(x.id!==id+'Box')x.classList.remove('open')});$(id+'Box').classList.toggle('open');setTimeout(()=>$(id+'Menu').querySelector('.multi-search')?.focus(),0)};$(id+'Menu').onclick=e=>e.stopPropagation()}
function refreshOptions(){const all=[...DATA.resumo_diario,...DATA.resumo_nodo,...DATA.resumo_placa,...DATA.detratores_rotas,...DATA.d0_rotas,...DATA.d1_rotas,...DATA.d1_acoes];fillMulti('regional',uniq(all,'regional'));fillMulti('facility',uniq(all,'facility'));fillMulti('tipo',uniq(all,'tipo'));fillMulti('transportadora',uniq(DATA.resumo_placa,'transportadora'));fillMulti('nodo',uniq([...DATA.resumo_nodo,...DATA.d1_acoes],'nodo'))}
function group(rows,key){const m=new Map();for(const r of rows){const k=r[key]||'SEM_INFO';const cur=m.get(k)||{label:k,ruteados:0,coletados:0,online:0,offline:0,nao_coletados:0};cur.ruteados+=n(r.ruteados);cur.coletados+=n(r.coletados);cur.online+=n(r.online);cur.offline+=n(r.offline);cur.nao_coletados+=n(r.nao_coletados);m.set(k,cur)}return [...m.values()].map(x=>({...x,pct_coleta:x.coletados/Math.max(x.ruteados,1)})).sort((a,b)=>a.pct_coleta-b.pct_coleta||b.nao_coletados-a.nao_coletados||b.ruteados-a.ruteados)}
function cards(rows){const r=sum(rows,'ruteados'),c=sum(rows,'coletados'),on=sum(rows,'online'),off=sum(rows,'offline'),nc=sum(rows,'nao_coletados');return '<section class="cards"><div class="card"><div class="num">'+fmt(r)+'</div><div class="lbl">Roteados</div></div><div class="card green"><div class="num">'+fmt(c)+'</div><div class="lbl">Coletados</div></div><div class="card"><div class="num">'+fmt(on)+'</div><div class="lbl">Online</div></div><div class="card purple"><div class="num">'+fmt(off)+'</div><div class="lbl">Offline</div></div><div class="card red"><div class="num">'+fmt(nc)+'</div><div class="lbl">Nao coletados</div></div><div class="card orange"><div class="num">'+pct(c/Math.max(r,1))+'</div><div class="lbl">% coleta</div></div><div class="card"><div class="num">'+pct(on/Math.max(c,1))+' / '+pct(off/Math.max(c,1))+'</div><div class="lbl">% online / % offline</div></div></section>'}
function perfClass(v){return v<.4?'red':v<.7?'orange':'green'}
function bars(title,rows,key){const id=exportKey(title+'_'+key),all=group(rows,key),size=15,total=Math.max(1,Math.ceil(all.length/size)),page=Math.min(Math.max(BAR_PAGES[id]||0,0),total-1);BAR_PAGES[id]=page;const g=all.slice(page*size,page*size+size);return '<div class="panel"><h2>'+esc(title)+'<span class="pager"><button onclick="barPage(&quot;'+id+'&quot;,-1)">‹</button><span>'+fmt(page+1)+' / '+fmt(total)+'</span><button onclick="barPage(&quot;'+id+'&quot;,1)">›</button></span></h2><div class="rows">'+g.map(x=>'<div class="bar" title="'+fmt(x.coletados)+' coletados de '+fmt(x.ruteados)+' roteados"><b>'+esc(x.label)+'</b><div class="track"><div class="fill '+perfClass(x.pct_coleta)+'" style="width:'+(x.pct_coleta*100)+'%"></div></div><b class="right">'+pct(x.pct_coleta)+'<small>'+fmt(x.coletados)+' / '+fmt(x.ruteados)+'</small></b></div>').join('')+'</div></div>'}
function barPage(id,dir){BAR_PAGES[id]=Math.max(0,(BAR_PAGES[id]||0)+dir);render()}
function chartTrend(rows){const m=new Map();for(const r of rows){const k=String(r.data);const cur=m.get(k)||{label:k,ruteados:0,coletados:0,online:0,offline:0,nao_coletados:0};cur.ruteados+=n(r.ruteados);cur.coletados+=n(r.coletados);cur.online+=n(r.online);cur.offline+=n(r.offline);cur.nao_coletados+=n(r.nao_coletados);m.set(k,cur)}const items=[...m.values()].sort((a,b)=>String(a.label).localeCompare(String(b.label))).slice(-30),max=Math.max(1,...items.map(x=>x.ruteados));return '<div class="panel"><h2>Coleta por dia <span class="status">azul online | roxo offline | cinza nao coletado</span></h2><div class="chart">'+items.map(d=>{const h=Math.max(4,d.ruteados/max*170),on=d.online/Math.max(d.ruteados,1)*h,off=d.offline/Math.max(d.ruteados,1)*h,fail=Math.max(1,d.nao_coletados/Math.max(d.ruteados,1)*h),p=d.coletados/Math.max(d.ruteados,1);return '<div class="day" title="'+fmt(d.ruteados)+' roteados | '+fmt(d.coletados)+' coletados | '+fmt(d.nao_coletados)+' nao coletados"><div class="chart-label">'+pct(p)+'</div><div class="chart-count">'+fmt(d.coletados)+'/'+fmt(d.ruteados)+'</div><div class="stack" style="height:'+h+'px"><div class="seg fail" style="height:'+fail+'px"></div><div class="seg offline" style="height:'+off+'px"></div><div class="seg online" style="height:'+on+'px"></div></div><span>'+esc(d.label.slice(5))+'</span></div>'}).join('')+'</div></div>'}
function aggregateTable(rows,keys){const m=new Map();for(const r of rows){const id=keys.map(k=>r[k]||'SEM_INFO').join('||');let cur=m.get(id);if(!cur){cur={};keys.forEach(k=>cur[k]=r[k]||'SEM_INFO');cur.ruteados=0;cur.coletados=0;cur.online=0;cur.offline=0;cur.nao_coletados=0;cur.qtd_rotas=0;cur._rotas=new Set();m.set(id,cur)}cur.ruteados+=n(r.ruteados);cur.coletados+=n(r.coletados);cur.online+=n(r.online);cur.offline+=n(r.offline);cur.nao_coletados+=n(r.nao_coletados);if(r.rota)cur._rotas.add(r.rota);cur.qtd_rotas+=n(r.qtd_rotas)}return [...m.values()].map(r=>{if(!r.qtd_rotas)r.qtd_rotas=r._rotas.size||1;delete r._rotas;r.pct_coleta=r.coletados/Math.max(r.ruteados,1);return r}).sort((a,b)=>a.pct_coleta-b.pct_coleta||b.nao_coletados-a.nao_coletados||b.ruteados-a.ruteados)}
function table(title,rows,kind){const cols=kind==='svc'?[['regional','Regional'],['tipo','Tipo coleta'],['facility','SVC']]:kind==='nodo'?[['regional','Regional'],['facility','SVC'],['tipo','Tipo coleta'],['nodo','Nodo'],['nome_nodo','Nome nodo']]:kind==='placa'?[['regional','Regional'],['facility','SVC'],['tipo','Tipo coleta'],['transportadora','Transportadora'],['placa','Placa']]:[['data','Data'],['regional','Regional'],['facility','SVC'],['tipo','Tipo coleta'],['rota','Rota'],['nodo','Nodo'],['nome_nodo','Nome nodo'],['transportadora','Transportadora'],['placa','Placa']];const tableRows=kind==='svc'?aggregateTable(rows,['regional','tipo','facility']):kind==='nodo'?aggregateTable(rows,['regional','facility','tipo','nodo','nome_nodo']):kind==='placa'?aggregateTable(rows,['regional','facility','tipo','transportadora','placa']):rows.slice().sort((a,b)=>n(b.nao_coletados)-n(a.nao_coletados)||n(b.ruteados)-n(a.ruteados));const view=tableRows.slice(0,500),key=exportKey(title+'_'+kind);EXPORTS[key]=tableRows;return '<div class="panel"><h2>'+esc(title)+exportButton(key)+' <span class="status">'+fmt(view.length)+' de '+fmt(tableRows.length)+' linhas exibidas</span></h2><div class="table-wrap"><table class="table"><thead><tr><th>#</th>'+cols.map(c=>'<th>'+c[1]+'</th>').join('')+'<th class="right">Rotas</th><th class="right">Roteados</th><th class="right">Coletados</th><th class="right">Online</th><th class="right">Offline</th><th class="right">Nao col.</th><th class="right">% coleta</th><th class="perf">Desempenho</th></tr></thead><tbody>'+view.map((r,i)=>{const p=n(r.coletados)/Math.max(n(r.ruteados),1),badge=p<.4?'bad':p<.7?'mid':'ok';return '<tr><td><span class="rank '+(i<3?'top':'')+'">'+(i+1)+'</span></td>'+cols.map(([k])=>'<td><b>'+esc(r[k]??'')+'</b></td>').join('')+'<td class="right">'+fmt(r.qtd_rotas||1)+'</td><td class="right">'+fmt(r.ruteados)+'</td><td class="right ok">'+fmt(r.coletados)+'</td><td class="right">'+fmt(r.online)+'</td><td class="right">'+fmt(r.offline)+'</td><td class="right bad">'+fmt(r.nao_coletados)+'</td><td class="right"><span class="badge '+badge+'">'+pct(p)+'</span></td><td class="perf"><div class="track"><div class="fill '+perfClass(p)+'" style="width:'+(p*100)+'%"></div></div></td></tr>'}).join('')+'</tbody></table></div></div>'}
function pocCompare(rows){const g=group(rows.map(r=>({...r,poc_grupo:DEDICADOS.includes(r.facility)?'POC dedicado':'Sem dedicado'})),'poc_grupo').sort((a,b)=>a.label.localeCompare(b.label)),key='comparativo_poc';EXPORTS[key]=g;return '<div class="panel"><h2>Comparativo POC dedicado vs demais SVCs'+exportButton(key)+'</h2><div class="table-wrap"><table class="table"><thead><tr><th>Grupo</th><th class="right">Roteados</th><th class="right">Coletados</th><th class="right">Nao col.</th><th class="right">% coleta</th><th class="perf">Desempenho</th></tr></thead><tbody>'+g.map(r=>{const badge=r.pct_coleta<.4?'bad':r.pct_coleta<.7?'mid':'ok';return '<tr><td><b>'+esc(r.label)+'</b></td><td class="right">'+fmt(r.ruteados)+'</td><td class="right ok">'+fmt(r.coletados)+'</td><td class="right bad">'+fmt(r.nao_coletados)+'</td><td class="right"><span class="badge '+badge+'">'+pct(r.pct_coleta)+'</span></td><td class="perf"><div class="track"><div class="fill '+perfClass(r.pct_coleta)+'" style="width:'+(r.pct_coleta*100)+'%"></div></div></td></tr>'}).join('')+'</tbody></table></div></div>'}
function pocTable(rows){const tableRows=aggregateTable(rows,['regional','tipo','facility']).map(r=>({...r,grupo:DEDICADOS.includes(r.facility)?'POC dedicado':'Sem dedicado'})).sort((a,b)=>(a.grupo===b.grupo?0:a.grupo==='POC dedicado'?-1:1)||a.pct_coleta-b.pct_coleta||b.nao_coletados-a.nao_coletados);const view=tableRows.slice(0,500),key='poc_por_svc';EXPORTS[key]=tableRows;return '<div class="panel"><h2>POC por SVC'+exportButton(key)+' <span class="status">'+fmt(view.length)+' de '+fmt(tableRows.length)+' linhas exibidas</span></h2><div class="table-wrap"><table class="table"><thead><tr><th>#</th><th>Grupo</th><th>Regional</th><th>Tipo coleta</th><th>SVC</th><th class="right">Roteados</th><th class="right">Coletados</th><th class="right">Nao col.</th><th class="right">% coleta</th><th class="perf">Desempenho</th></tr></thead><tbody>'+view.map((r,i)=>{const badge=r.pct_coleta<.4?'bad':r.pct_coleta<.7?'mid':'ok';return '<tr><td><span class="rank '+(i<3?'top':'')+'">'+(i+1)+'</span></td><td><b>'+esc(r.grupo)+'</b></td><td>'+esc(r.regional)+'</td><td>'+esc(r.tipo)+'</td><td><b>'+esc(r.facility)+'</b></td><td class="right">'+fmt(r.ruteados)+'</td><td class="right ok">'+fmt(r.coletados)+'</td><td class="right bad">'+fmt(r.nao_coletados)+'</td><td class="right"><span class="badge '+badge+'">'+pct(r.pct_coleta)+'</span></td><td class="perf"><div class="track"><div class="fill '+perfClass(r.pct_coleta)+'" style="width:'+(r.pct_coleta*100)+'%"></div></div></td></tr>'}).join('')+'</tbody></table></div></div>'}
function pocView(){const rows=filtered(DATA.resumo_diario),dedicados=rows.filter(r=>DEDICADOS.includes(r.facility)),semDedicado=rows.filter(r=>!DEDICADOS.includes(r.facility));return cards(rows)+'<section>'+chartTrend(rows)+'</section><br><section class="grid2">'+pocCompare(rows)+bars('POC SVCs dedicados',dedicados,'facility')+'</section><br><section>'+bars('Sem dedicado por SVC',semDedicado,'facility')+'</section><br>'+pocTable(rows)}
function detailTable(title,rows){const tableRows=rows.slice().sort((a,b)=>n(b.nao_coletados)-n(a.nao_coletados)||n(b.ruteados)-n(a.ruteados)),view=tableRows.slice(0,500),key=exportKey(title);EXPORTS[key]=tableRows;const priClass=p=>p==='ALTA'?'bad':p==='MEDIA'?'mid':p==='OK'?'ok':'mid';return '<div class="panel"><h2>'+esc(title)+exportButton(key)+' <span class="status">'+fmt(view.length)+' de '+fmt(tableRows.length)+' nodos exibidos</span></h2><div class="table-wrap" style="max-height:620px"><table class="table"><thead><tr><th>#</th><th>Prioridade</th><th>Nodo</th><th>Nome nodo</th><th>SVC</th><th>Tipo coleta</th><th>Regional</th><th>Rotas sem coleta</th><th class="right">Roteados</th><th class="right">Coletados</th><th class="right">Online</th><th class="right">Offline</th><th class="right">Nao col.</th><th class="right">% coleta</th><th>Comentario</th></tr></thead><tbody>'+view.map((r,i)=>'<tr><td><span class="rank '+(i<3?'top':'')+'">'+(i+1)+'</span></td><td><span class="badge '+priClass(r.prioridade)+'">'+esc(r.prioridade)+'</span></td><td><b>'+esc(r.nodo)+'</b></td><td>'+esc(r.nome_nodo)+'</td><td><b>'+esc(r.facility)+'</b></td><td>'+esc(r.tipo)+'</td><td>'+esc(r.regional)+'</td><td>'+esc(r.rotas_sem_coleta||'nenhuma')+'</td><td class="right">'+fmt(r.ruteados)+'</td><td class="right ok">'+fmt(r.coletados)+'</td><td class="right">'+fmt(r.online)+'</td><td class="right">'+fmt(r.offline)+'</td><td class="right bad">'+fmt(r.nao_coletados)+'</td><td class="right">'+pct(r.pct_coleta)+'</td><td>'+esc(r.comentario)+'</td></tr>').join('')+'</tbody></table></div></div>'}
function source(){const sqls=Object.entries(DATA.queryTexts||{}).map(([name,sql])=>'<details style="margin:10px 0"><summary style="cursor:pointer;font-weight:900">'+esc(name)+'</summary><pre style="white-space:pre-wrap;background:#f7f9fc;border:1px solid #d8dee8;border-radius:8px;padding:12px;max-height:420px;overflow:auto">'+esc(sql)+'</pre></details>').join('');return '<div class="panel"><h2>Fonte e logica</h2><div class="rows" style="max-height:none"><h3>Base e regra geral</h3><p><b>Base:</b> meli-bi-data.SBOX_MLBPLACES.DETRATORES_RTS_BASE.</p><p><b>Origem:</b> BT_SHP_SHIPMENTS_LAST_MILE_PICKUP + BT_MLB_LAST_MILE + BT_CARTEIRA_MLB.</p><p><b>Tipo coleta:</b> prioriza SHP_NODE_ID_TYPE da LAST_MILE_PICKUP; a carteira entra apenas como fallback quando a fonte nao trouxer o tipo.</p><p><b>Regra:</b> Roteado = PUS_LM_IS_RUTEADO = 1. Coletado = PUS_LM_IS_COLLECTED = 1. Offline = coletado com PUS_LM_IS_COLLECTED_OFFLINE = 1. Online = coletado sem flag offline. Nao coletado = pacote roteado sem coleta.</p><p><b>Contagem:</b> as visoes usam pacote unico por agrupamento, com COUNT(DISTINCT shipment_id), para evitar inflar o volume por reprocessamento ou tentativa duplicada.</p><h3>Abas</h3><p><b>Por SVC, Por Nodo, Por Placa e Por Rota:</b> aberturas operacionais dos ultimos dias.</p><p><b>D0:</b> ultima data disponivel na base. <b>D-1:</b> dia anterior ao D0. <b>D-1 Detalhe:</b> prioridades por nodo calculadas direto da fonte para o D-1 operacional.</p><p><b>POC Dedicado:</b> compara os SVCs SSP6, SSP11 e SMG6 contra os demais SVCs usando a mesma base deduplicada.</p><h3>Atualizacao</h3><p><b>Hora fonte:</b> MAX(source_updated_at). <b>Base painel:</b> MAX(refreshed_at).</p><h3>SQLs</h3>'+sqls+'</div></div>'}
function render(){if(tab==='fonte'){view.innerHTML=source();return}const rows=filtered();if(tab==='poc'){view.innerHTML=pocView();return}if(tab==='d1acoes'){view.innerHTML=cards(rows)+'<section class="grid2">'+bars('D-1 Detalhe por prioridade',rows,'prioridade')+bars('D-1 Detalhe por nodo',rows,'nodo')+'</section><br>'+detailTable('D-1 Detalhe - prioridades por nodo',rows);return}const metricRows=(tab==='d0'||tab==='d1')?rows:filtered(DATA.resumo_diario);let html=cards(metricRows)+'<section>'+chartTrend(metricRows)+'</section><br>';if(tab==='svc')html+='<section class="grid2">'+bars('Piores SVCs',rows,'facility')+bars('Piores nodos',filtered(DATA.resumo_nodo),'nodo')+'</section><br>'+table('Por SVC',rows,'svc');if(tab==='nodo')html+='<section class="grid2">'+bars('Piores nodos',rows,'nodo')+bars('Piores nomes de nodo',rows,'nome_nodo')+'</section><br>'+table('Por Nodo',rows,'nodo');if(tab==='placa')html+='<section class="grid2">'+bars('Piores transportadoras',rows,'transportadora')+bars('Piores placas',rows,'placa')+'</section><br>'+table('Por Placa',rows,'placa');if(tab==='rota')html+='<section class="grid2">'+bars('Piores rotas',rows,'rota')+bars('Piores transportadoras',rows,'transportadora')+'</section><br>'+table('Por Rota',rows,'rota');if(tab==='d0')html+='<section class="grid2">'+bars('D0 por SVC',rows,'facility')+bars('D0 por Nodo',rows,'nodo')+'</section><br>'+table('D0 - rotas da ultima data carregada',rows,'rota');if(tab==='d1')html+='<section class="grid2">'+bars('D-1 por SVC',rows,'facility')+bars('D-1 por Nodo',rows,'nodo')+'</section><br>'+table('D-1 - rotas do dia anterior ao D0',rows,'rota');view.innerHTML=html}
document.querySelectorAll('.nav button').forEach(b=>b.onclick=()=>{document.querySelectorAll('.nav button').forEach(x=>x.classList.remove('active'));b.classList.add('active');tab=b.dataset.tab;render()});
['de','ate','busca'].forEach(id=>$(id).addEventListener('input',render));
$('limpar').onclick=()=>{['regional','facility','tipo','transportadora','nodo'].forEach(id=>setMulti(id,[]));$('de').value=DEFAULT_DE;$('ate').value=DEFAULT_ATE;$('busca').value='';render()};
$('exportPacotes').onclick=exportPacotes;
document.addEventListener('click',()=>document.querySelectorAll('.multi.open').forEach(x=>x.classList.remove('open')));
const view=document.getElementById('view');
loadData().then(p=>{p=withRequiredDatasets(p);DATA=p.datasets;DATA.validation=p.validation;DATA.queryTexts=p.queryTexts;normalizeDatasets();syncMetadata();refreshOptions();['regional','facility','tipo','transportadora','nodo'].forEach(bindMulti);render()});
</script></body></html>`;
}

let datasets;
let validation;
let queryTexts;

if (process.env.REUSE_OUTPUT_PAYLOAD === '1') {
  console.log('Reutilizando payload existente do HTML local...');
  const existing = await loadExistingPayload();
  datasets = existing.datasets;
  validation = existing.validation;
  queryTexts = existing.queryTexts;
} else {
  console.log('Materializando base deduplicada...');
  await runBq(await fs.readFile('query_detratores_rts_materializar_base.sql', 'utf8'), '1');

  datasets = {};
  for (const [name, file] of Object.entries(queries)) {
    console.log(`Rodando ${name}...`);
    datasets[name] = await runBq(await fs.readFile(file, 'utf8'));
    console.log(`${name}: ${datasets[name].length} linhas`);
  }

  const validationRows = await runBq(`
SELECT
  COUNT(*) AS linhas,
  COUNTIF(transportadora = 'SEM_TRANSPORTADORA') AS sem_transportadora,
  COUNTIF(nome_nodo = 'SEM_NODO') AS sem_nome_nodo,
  (SELECT FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', MAX(COALESCE(AUD_UPD_DTTM, AUD_INS_DTTM)))
   FROM \`meli-bi-data.WHOWNER.BT_SHP_SHIPMENTS_LAST_MILE_PICKUP\`
   WHERE SHP_SITE_ID = 'MLB') AS fonte_atualizada_em,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', MAX(refreshed_at), 'America/Sao_Paulo') AS atualizado_em
FROM \`meli-bi-data.SBOX_MLBPLACES.DETRATORES_RTS_BASE\`
`);
  validation = {
    linhas: num(validationRows[0]?.linhas),
    sem_transportadora: num(validationRows[0]?.sem_transportadora),
    sem_nome_nodo: num(validationRows[0]?.sem_nome_nodo),
    fonte_atualizada_em: validationRows[0]?.fonte_atualizada_em,
    atualizado_em: validationRows[0]?.atualizado_em
  };
  queryTexts = {
    materializar_base: await fs.readFile('query_detratores_rts_materializar_base.sql', 'utf8')
  };
  for (const [name, file] of Object.entries(queries)) {
    queryTexts[name] = await fs.readFile(file, 'utf8');
  }
}

const logoSrc = await loadLogoSrc();
await fs.mkdir('output', { recursive: true });
if (process.env.REUSE_OUTPUT_PAYLOAD !== '1') {
  await fs.writeFile(
    'output/detratores-rts-payload.json',
    JSON.stringify({ datasets, validation, queryTexts }),
    'utf8'
  );
}
await fs.writeFile(
  'output/detratores-rts-dataset-payload.json',
  JSON.stringify({
    datasets: compactDatasetsForLiveDataset(datasets),
    validation
  }),
  'utf8'
);
const liveDatasets = compactDatasetsForLiveDataset(datasets);
await fs.mkdir('output/grid-datasets', { recursive: true });
await fs.writeFile('output/grid-datasets/detratores_rts_meta.json', JSON.stringify([validation]), 'utf8');
const datasetFileMap = {
  resumo_diario: 'detratores_rts_resumo_diario.json',
  resumo_nodo: 'detratores_rts_resumo_nodo.json',
  resumo_placa: 'detratores_rts_resumo_placa.json',
  detratores_rotas: 'detratores_rts_rotas_30d.json',
  d0_rotas: 'detratores_rts_d0_rotas.json',
  d1_rotas: 'detratores_rts_d1_rotas.json',
  d1_acoes: 'detratores_rts_d1_detalhe.json',
  pacotes: 'detratores_rts_pacotes_d0_d1.json'
};
for (const [key, file] of Object.entries(datasetFileMap)) {
  await fs.writeFile(`output/grid-datasets/${file}`, JSON.stringify(liveDatasets[key] || []), 'utf8');
}
const html = buildHtml({ datasets, logoSrc, validation, queryTexts });
await fs.writeFile('output/detratores-rts-grid.html', html, 'utf8');
console.log(`HTML gerado: output/detratores-rts-grid.html (${Buffer.byteLength(html, 'utf8')} bytes)`);

if (process.env.PUBLISH_GRID === '1') {
  const result = await publishGridWithRetry(html);
  console.log(`Publicado: https://grid.adminml.com/d/${result.doc_id || DOC_ID}/view`);
}
