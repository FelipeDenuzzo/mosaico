const fs = require('fs');
const path = require('path');
let chokidar = null;
try { chokidar = require('chokidar'); } catch(e) {}
const http = require('http');

const ROOT = path.resolve(__dirname);
const OUTPUT_DIR = path.join(ROOT, 'Output');
const MANIFEST_PATH = path.join(ROOT, 'manifest.json');
const LOGS_DIR = path.join(ROOT, 'logs');

if (!fs.existsSync(LOGS_DIR)) {
  try { fs.mkdirSync(LOGS_DIR, { recursive: true }); } catch(e) {}
}
const LOG_FILE = path.join(LOGS_DIR, 'watch_manifest.log');

function log(msg) {
  const ts = new Date().toISOString().replace('T', ' ').substring(0, 19);
  const line = `[${ts}] ${msg}`;
  console.log(line);
  try {
    fs.appendFileSync(LOG_FILE, line + '\n', 'utf8');
  } catch(e) {}
}

const VALID_EXT = new Set(['.jpg', '.jpeg', '.png', '.webp', '.avif']);
const MAX_MOSAICS = 10; // Mantém até 10 mosaicos recentes na parede principal
const PORT = 8081;

let state = { mosaics: [], queue: [], seen: [], isBusy: false, isBusyTimestamp: 0 };

function readCurrentManifest() {
  try {
    if (!fs.existsSync(MANIFEST_PATH)) {
      return { mosaics: [], queue: [], seen: [], isBusy: false, isBusyTimestamp: 0 };
    }
    const raw = fs.readFileSync(MANIFEST_PATH, 'utf8');
    const json = JSON.parse(raw);
    return {
      mosaics: Array.isArray(json.mosaics) ? json.mosaics : [],
      queue: Array.isArray(json.queue) ? json.queue : [],
      seen: Array.isArray(json.seen) ? json.seen : [],
      isBusy: typeof json.isBusy === 'boolean' ? json.isBusy : false,
      isBusyTimestamp: typeof json.isBusyTimestamp === 'number' ? json.isBusyTimestamp : 0
    };
  } catch (e) {
    return { mosaics: [], queue: [], seen: [], isBusy: false, isBusyTimestamp: 0 };
  }
}

function saveManifest() {
  const newJson = JSON.stringify(state, null, 2);
  try {
    fs.writeFileSync(MANIFEST_PATH, newJson, 'utf8');
    log(`[watch-manifest] Manifest atualizado: ${state.mosaics.length} exibidos, ${state.queue.length} na fila.`);
  } catch(e) {
    log(`[watch-manifest] Erro ao salvar manifest: ${e.message}`);
  }
}

function syncWithFolder(isStartup = false) {
  if (!fs.existsSync(OUTPUT_DIR)) {
    try { fs.mkdirSync(OUTPUT_DIR, { recursive: true }); } catch(e) {}
    return;
  }
  const entries = fs.readdirSync(OUTPUT_DIR, { withFileTypes: true });

  const validFiles = new Map();
  const fileList = [];

  for (const entry of entries) {
    if (!entry.isFile()) continue;
    const ext = path.extname(entry.name).toLowerCase();
    if (!VALID_EXT.has(ext)) continue;

    const fullPath = path.join(OUTPUT_DIR, entry.name);
    try {
      const stat = fs.statSync(fullPath);
      let recentX = null;
      let recentY = null;
      const jsonPath = fullPath + '.json';
      if (fs.existsSync(jsonPath)) {
        try {
          const jsonData = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
          if (jsonData.recent_x !== undefined) recentX = jsonData.recent_x;
          if (jsonData.recent_y !== undefined) recentY = jsonData.recent_y;
        } catch(e) {}
      }

      const item = {
        file: `/Output/${encodeURIComponent(entry.name)}`,
        name: entry.name,
        createdAt: stat.mtime.toISOString(),
        mtimeMs: stat.mtimeMs,
        ...(recentX !== null && { recentX }),
        ...(recentY !== null && { recentY })
      };
      validFiles.set(entry.name, item);
      fileList.push(item);
    } catch(e) {}
  }

  // Ordena decrescente: o mais recente primeiro
  fileList.sort((a, b) => b.mtimeMs - a.mtimeMs);

  const oldStateStr = JSON.stringify(state);

  if (isStartup) {
    // Ao iniciar: carrega os mosaicos já existentes em Output/ diretamente na parede
    log(`[watch-manifest] Startup: ${fileList.length} mosaicos existentes detectados em Output/.`);
    state.mosaics = fileList.slice(0, MAX_MOSAICS);
    state.seen = fileList.slice(MAX_MOSAICS).map(f => f.name);
    state.queue = [];
    state.isBusy = false;
    state.isBusyTimestamp = 0;
  } else {
    // Modo watcher regular
    state.mosaics = state.mosaics.filter(m => validFiles.has(m.name));
    state.queue = state.queue.filter(m => validFiles.has(m.name));
    if (!state.seen) state.seen = [];
    state.seen = state.seen.filter(name => validFiles.has(name));

    const existingNames = new Set([
      ...state.mosaics.map(m => m.name),
      ...state.queue.map(m => m.name),
      ...state.seen
    ]);

    const novos = [];
    for (const item of fileList) {
      if (!existingNames.has(item.name)) {
        novos.push(item);
      }
    }

    if (novos.length > 0) {
      log(`[watch-manifest] ${novos.length} novos mosaicos adicionados à fila.`);
      state.queue.push(...novos);
      state.queue.sort((a, b) => b.mtimeMs - a.mtimeMs);
      state.isBusy = true;
      state.isBusyTimestamp = Date.now();
    }

    // Se houver vaga na parede e itens na fila, sobe automaticamente
    while (state.mosaics.length < MAX_MOSAICS && state.queue.length > 0) {
      const nextItem = state.queue.shift();
      state.mosaics.push(nextItem);
    }
  }

  if (JSON.stringify(state) !== oldStateStr) {
    saveManifest();
  }
}

state = readCurrentManifest();
log(`[watch-manifest] Estado inicial carregado da memoria: Parede: ${state.mosaics.length}, Fila: ${state.queue.length}`);
syncWithFolder(true); // Inicialização segura
log(`[watch-manifest] Estado inicial apos sync: Parede: ${state.mosaics.length}, Fila: ${state.queue.length}`);

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
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
        let currentConfig = { intervalSeconds: 60, cameraIndex: 1 };
        if (fs.existsSync(configPath)) {
          try {
            currentConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'));
          } catch(e) {}
        }
        currentConfig.intervalSeconds = intervalSeconds;
        if (data.cameraIndex !== undefined && !isNaN(parseInt(data.cameraIndex, 10))) {
          currentConfig.cameraIndex = parseInt(data.cameraIndex, 10);
        }
        fs.writeFileSync(configPath, JSON.stringify(currentConfig, null, 2), 'utf8');
        log(`[watch-manifest] Configuração da câmera salva: ${intervalSeconds}s (Índice da câmera: ${currentConfig.cameraIndex})`);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, ...currentConfig }));
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: 'JSON inválido' }));
      }
    });
  } else if (req.method === 'GET' && req.url === '/config-exibicao') {
    const configPath = path.join(ROOT, 'exibicao_config.json');
    if (fs.existsSync(configPath)) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(fs.readFileSync(configPath, 'utf8'));
    } else {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: false, error: 'Configuração não encontrada' }));
    }
  } else if (req.method === 'POST' && req.url === '/config-exibicao') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const configPath = path.join(ROOT, 'exibicao_config.json');
        fs.writeFileSync(configPath, body, 'utf8');
        log(`[watch-manifest] Configuração de exibição salva no disco.`);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true }));
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: 'Erro ao salvar' }));
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
    state.isBusyTimestamp = Date.now();
    saveManifest();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, isBusy: true }));
  } else if (req.method === 'POST' && req.url === '/idle') {
    state.isBusy = false;
    state.isBusyTimestamp = 0;
    saveManifest();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, isBusy: false }));
  } else {
    res.writeHead(404);
    res.end();
  }
});

server.listen(PORT, () => {
    log(`[watch-manifest] Endpoint rodando na porta ${PORT}`);
});

if (chokidar) {
  const watcher = chokidar.watch(OUTPUT_DIR, { persistent: true, ignoreInitial: true, depth: 0 });
  watcher
    .on('add', (filePath) => {
      log(`[watch-manifest] Novo mosaico detectado: ${path.basename(filePath)}`);
      syncWithFolder();
    })
    .on('unlink', (filePath) => {
      log(`[watch-manifest] Mosaico removido: ${path.basename(filePath)}`);
      syncWithFolder();
    })
    .on('error', (e) => log(`[watch-manifest] Erro watcher: ${e.message}`));
  log('[watch-manifest] Monitorando Output/ com chokidar.');
} else {
  log('[watch-manifest] chokidar nao encontrado; usando fs.watch nativo do Node.');
  if (fs.existsSync(OUTPUT_DIR)) {
    let debounceTimer = null;
    fs.watch(OUTPUT_DIR, (eventType, filename) => {
      if (!filename) return;
      const ext = path.extname(filename).toLowerCase();
      if (!VALID_EXT.has(ext)) return;
      if (debounceTimer) clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => {
        log(`[watch-manifest] Atualizacao detectada em Output/: ${filename}`);
        syncWithFolder();
      }, 500);
    });
  }
}
