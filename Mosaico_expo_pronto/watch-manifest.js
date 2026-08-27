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

let state = { mosaics: [], queue: [], seen: [], isBusy: false, isBusyTimestamp: 0 };

function readCurrentManifest() {
  try {
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

    validFiles.set(entry.name, {
      file: `/Output/${encodeURIComponent(entry.name)}`,
      name: entry.name,
      createdAt: stat.mtime.toISOString(),
      ...(recentX !== null && { recentX }),
      ...(recentY !== null && { recentY })
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
    state.isBusyTimestamp = 0;
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
    state.isBusy = true;
    state.isBusyTimestamp = Date.now();
  }

  while (state.mosaics.length < MAX_MOSAICS && state.queue.length > 0) {
    state.mosaics.push(state.queue.shift());
  }

  // Manter no disco apenas os arquivos que estão ativos na parede (mosaics) ou na fila (queue)
  const activeNames = new Set([
    ...state.mosaics.map(m => m.name),
    ...state.queue.map(m => m.name)
  ]);

  for (const name of validFiles.keys()) {
    if (!activeNames.has(name)) {
      const removePath = path.join(OUTPUT_DIR, name);
      try {
        if (fs.existsSync(removePath)) {
          fs.unlinkSync(removePath);
          validFiles.delete(name);
          console.log(`[watch-manifest] Removido arquivo excedente do Output/: ${name}`);
        }
      } catch(e) {
        console.warn(`[watch-manifest] Erro ao remover do Output/: ${name}:`, e.message);
      }
    }
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
        console.log(`[watch-manifest] Configuração da câmera salva: ${intervalSeconds}s (Índice da câmera: ${currentConfig.cameraIndex})`);
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
        console.log(`[watch-manifest] Configuração de exibição salva no disco.`);
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

         // Deleta o arquivo físico rotacionado para manter somente os últimos 5
         const removePath = path.join(OUTPUT_DIR, removed.name);
         if (fs.existsSync(removePath)) {
           try {
             fs.unlinkSync(removePath);
             console.log(`[watch-manifest] Mosaico antigo removido do disco: ${removed.name}`);
           } catch(e) {
             console.error(`[watch-manifest] Erro ao remover mosaico antigo:`, e.message);
           }
         }
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
