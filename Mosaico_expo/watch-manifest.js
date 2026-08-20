const fs = require('fs');
const path = require('path');
const chokidar = require('chokidar');
const http = require('http');

const ROOT = path.resolve(__dirname);
const OUTPUT_DIR = path.join(ROOT, 'Output');
const MANIFEST_PATH = path.join(ROOT, 'manifest.json');

const VALID_EXT = new Set(['.jpg', '.jpeg', '.png', '.webp', '.avif']);
const MAX_MOSAICS = 5;
const PORT = 8081;

let state = { mosaics: [], queue: [], seen: [], isBusy: false };

function readCurrentManifest() {
  try {
    const raw = fs.readFileSync(MANIFEST_PATH, 'utf8');
    const json = JSON.parse(raw);
    return {
      mosaics: Array.isArray(json.mosaics) ? json.mosaics : [],
      queue: Array.isArray(json.queue) ? json.queue : [],
      seen: Array.isArray(json.seen) ? json.seen : [],
      isBusy: typeof json.isBusy === 'boolean' ? json.isBusy : false
    };
  } catch (e) {
    return { mosaics: [], queue: [], seen: [], isBusy: false };
  }
}

function saveManifest() {
  const newJson = JSON.stringify(state, null, 2);
  try {
    fs.writeFileSync(MANIFEST_PATH, newJson, 'utf8');
    console.log(`[watch-manifest] Manifest atualizado: ${state.mosaics.length} exibidos, ${state.queue.length} na fila.`);
  } catch(e) {
    console.error('[watch-manifest] Erro ao salvar manifest:', e);
  }
}

function syncWithFolder(isStartup = false) {
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
  
  if (isStartup) {
    // Ao iniciar, zeramos a fila antiga. Não queremos herdar fantasmas.
    if (state.queue && state.queue.length > 0) {
      if (!state.seen) state.seen = [];
      state.seen.push(...state.queue.map(q => q.name));
    }
    state.queue = [];
    state.isBusy = false; // Reset busy status on boot
  } else {
    state.queue = state.queue.filter(m => validFiles.has(m.name));
  }

  if (!state.seen) state.seen = [];
  state.seen = state.seen.filter(name => validFiles.has(name));

  const existingNames = new Set([
    ...state.mosaics.map(m => m.name),
    ...state.queue.map(m => m.name),
    ...state.seen
  ]);

  const novos = [];
  for (const [name, meta] of validFiles.entries()) {
    if (!existingNames.has(name)) {
      if (isStartup) {
        // Se encontrou arquivo no init que não é da parede, tratamos como 'visto'
        // para nunca criar fila no boot.
        state.seen.push(name);
      } else {
        // Apenas arquivos realmente novos criados durante o watcher vão para fila
        novos.push(meta);
      }
    }
  }

  novos.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  
  // Transição do estado inchado antigo:
  // Se já tínhamos mais mosaicos do que o permitido (ex: 102),
  // ajustamos agora: pegamos os excedentes e movemos para a lista de vistos 'seen'
  if (state.mosaics.length > MAX_MOSAICS) {
    const excess = state.mosaics.slice(0, state.mosaics.length - MAX_MOSAICS);
    const names = excess.map(m => m.name);
    state.seen.push(...names);
    state.mosaics = state.mosaics.slice(-MAX_MOSAICS);
  }

  if (novos.length > 0) {
    state.queue.push(...novos);
    state.queue.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  }

  while (state.mosaics.length < MAX_MOSAICS && state.queue.length > 0) {
    state.mosaics.push(state.queue.shift());
  }

  if (JSON.stringify(state) !== oldStateStr) {
    saveManifest();
  }
}

state = readCurrentManifest();
console.log(`[watch-manifest] Estado inicial carregado da memoria: Parede: ${state.mosaics.length}, Fila: ${state.queue.length}`);
syncWithFolder(true); // Passa true para o boot
console.log(`[watch-manifest] Estado inicial apos limpeza: Parede: ${state.mosaics.length}, Fila: ${state.queue.length}`);

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  if (req.method === 'POST' && req.url === '/config-camera') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const data = JSON.parse(body);
        const intervalSeconds = parseInt(data.intervalSeconds, 10);
        if (isNaN(intervalSeconds) || intervalSeconds < 5) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, error: 'Intervalo inválido. Mínimo 5 segundos.' }));
          return;
        }
        const configPath = path.join(ROOT, 'camera_config.json');
        fs.writeFileSync(configPath, JSON.stringify({ intervalSeconds }, null, 2), 'utf8');
        console.log(`[watch-manifest] Configuração da câmera salva: ${intervalSeconds}s`);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, intervalSeconds }));
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: 'JSON inválido' }));
      }
    });
  } else if (req.method === 'POST' && req.url === '/next') {
    if (state.queue.length > 0) {
      const nextItem = state.queue.shift();
      if (state.mosaics.length >= MAX_MOSAICS) {
         const removed = state.mosaics.shift();
         if (!state.seen) state.seen = [];
         state.seen.push(removed.name);
      }
      state.mosaics.push(nextItem);
      saveManifest();
      
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: true, rotated: nextItem.name, queue: state.queue.length }));
    } else {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: false, message: 'Fila vazia' }));
    }
  } else if (req.method === 'POST' && req.url === '/busy') {
    state.isBusy = true;
    saveManifest();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, isBusy: true }));
  } else if (req.method === 'POST' && req.url === '/idle') {
    state.isBusy = false;
    saveManifest();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, isBusy: false }));
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
