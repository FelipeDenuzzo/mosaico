const fs = require('fs');
const path = require('path');
const chokidar = require('chokidar');
const http = require('http');

const ROOT = path.resolve(__dirname, '..');
const OUTPUT_DIR = path.join(ROOT, 'Output');
const EXIBICAO_DIR = path.join(ROOT, 'Mosaico_exibicao');
const MANIFEST_PATH = path.join(EXIBICAO_DIR, 'manifest.json');
const SITE_EXIBICAO_DIR = path.join(ROOT, 'Site', 'Mosaico_exibicao');
const SITE_MANIFEST_PATH = path.join(SITE_EXIBICAO_DIR, 'manifest.json');

const VALID_EXT = new Set(['.jpg', '.jpeg', '.png', '.webp', '.avif']);
const MAX_MOSAICS = 5;
const PORT = 8081;

let state = { mosaics: [], queue: [] };

function readCurrentManifest() {
  try {
    const raw = fs.readFileSync(MANIFEST_PATH, 'utf8');
    const json = JSON.parse(raw);
    return {
      mosaics: Array.isArray(json.mosaics) ? json.mosaics : [],
      queue: Array.isArray(json.queue) ? json.queue : []
    };
  } catch (e) {
    return { mosaics: [], queue: [] };
  }
}

function saveManifest() {
  const newJson = JSON.stringify(state, null, 2);
  try {
    fs.writeFileSync(MANIFEST_PATH, newJson, 'utf8');
    if (fs.existsSync(SITE_EXIBICAO_DIR)) {
      fs.writeFileSync(SITE_MANIFEST_PATH, newJson, 'utf8');
    }
    console.log(`[watch-manifest] Manifest atualizado: ${state.mosaics.length} exibidos, ${state.queue.length} na fila.`);
  } catch(e) {
    console.error('[watch-manifest] Erro ao salvar manifest:', e);
  }
}

function syncWithFolder() {
  if (!fs.existsSync(OUTPUT_DIR)) return;
  const files = fs.readdirSync(OUTPUT_DIR, { withFileTypes: true });

  const validFiles = new Map();
  for (const entry of files) {
    if (!entry.isFile()) continue;
    const ext = path.extname(entry.name).toLowerCase();
    if (!VALID_EXT.has(ext)) continue;

    const fullPath = path.join(OUTPUT_DIR, entry.name);
    const stat = fs.statSync(fullPath);
    validFiles.set(entry.name, {
      file: `/Output/${encodeURIComponent(entry.name)}`,
      name: entry.name,
      createdAt: stat.mtime.toISOString()
    });
  }

  const oldStateStr = JSON.stringify(state);

  state.mosaics = state.mosaics.filter(m => validFiles.has(m.name));
  state.queue = state.queue.filter(m => validFiles.has(m.name));

  const existingNames = new Set([
    ...state.mosaics.map(m => m.name),
    ...state.queue.map(m => m.name)
  ]);

  const novos = [];
  for (const [name, meta] of validFiles.entries()) {
    if (!existingNames.has(name)) {
      novos.push(meta);
    }
  }

  novos.sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt));
  
  if (novos.length > 0) {
    state.queue.push(...novos);
  }

  while (state.mosaics.length < MAX_MOSAICS && state.queue.length > 0) {
    state.mosaics.push(state.queue.shift());
  }

  if (JSON.stringify(state) !== oldStateStr) {
    saveManifest();
  }
}

state = readCurrentManifest();
console.log(`[watch-manifest] Estado inicial: Parede: ${state.mosaics.length}, Fila: ${state.queue.length}`);
syncWithFolder();

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  if (req.method === 'POST' && req.url === '/next') {
    if (state.queue.length > 0) {
      const nextItem = state.queue.shift();
      if (state.mosaics.length >= MAX_MOSAICS) {
         state.mosaics.shift(); 
      }
      state.mosaics.push(nextItem);
      saveManifest();
      
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: true, rotated: nextItem.name, queue: state.queue.length }));
    } else {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: false, message: 'Fila vazia' }));
    }
  } else {
    res.writeHead(404);
    res.end();
  }
});

server.listen(PORT, () => {
    console.log(`[watch-manifest] Endpoint de rotacao rodando na porta ${PORT}`);
});

const watcher = chokidar.watch(OUTPUT_DIR, { persistent: true, ignoreInitial: true, depth: 0 });
watcher
  .on('add', (filePath) => {
    console.log('[watch-manifest] Novo mosaico detectado:', path.basename(filePath));
    syncWithFolder();
  })
  .on('unlink', (filePath) => {
    console.log('[watch-manifest] Mosaico removido:', path.basename(filePath));
    syncWithFolder();
  })
  .on('error', (e) => console.error('[watch-manifest] Erro watcher:', e));
