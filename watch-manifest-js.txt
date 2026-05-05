// watch-manifest.js
// Observa a pasta Output e mantém manifest.json sempre atualizado

const fs = require('fs');
const path = require('path');
const chokidar = require('chokidar'); // npm install chokidar

// Caminhos base
const ROOT = __dirname;
const OUTPUT_DIR = path.join(ROOT, 'Output');
const EXIBICAO_DIR = path.join(ROOT, 'Mosaico_exibicao');
const MANIFEST_PATH = path.join(EXIBICAO_DIR, 'manifest.json');

// Extensões válidas de mosaico
const VALID_EXT = new Set(['.jpg', '.jpeg', '.png', '.webp', '.avif']);

// Lê o manifest atual (se existir)
function readCurrentManifest() {
  try {
    const raw = fs.readFileSync(MANIFEST_PATH, 'utf8');
    const json = JSON.parse(raw);
    if (Array.isArray(json.mosaics)) return json;
  } catch (e) {
    // se não existe ou está inválido, começamos vazio
  }
  return { mosaics: [] };
}

// Gera o manifest a partir dos arquivos da pasta Output
function buildManifestFromFolder() {
  if (!fs.existsSync(OUTPUT_DIR)) {
    console.warn('[watch-manifest] Pasta Output não encontrada:', OUTPUT_DIR);
    return { mosaics: [] };
  }

  const files = fs.readdirSync(OUTPUT_DIR, { withFileTypes: true });
  const mosaics = [];

  for (const entry of files) {
    if (!entry.isFile()) continue;
    const ext = path.extname(entry.name).toLowerCase();
    if (!VALID_EXT.has(ext)) continue;

    const fullPath = path.join(OUTPUT_DIR, entry.name);
    const stat = fs.statSync(fullPath);

    mosaics.push({
      // Use caminho público absoluto relativo à raiz do site e encode do nome
      file: `/Output/${encodeURIComponent(entry.name)}`,
      name: entry.name,
      // createdAt baseado no mtime (última modificação)
      createdAt: stat.mtime.toISOString()
    });
  }

  // Ordena do mais recente para o mais antigo
  mosaics.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

  return { mosaics };
}

// Escreve manifest.json se houver mudança
function writeManifestIfChanged(newManifest) {
  const current = readCurrentManifest();
  const oldJson = JSON.stringify(current);
  const newJson = JSON.stringify(newManifest);

  if (oldJson === newJson) {
    console.log('[watch-manifest] manifest.json já está atualizado.');
    return;
  }

  fs.writeFileSync(MANIFEST_PATH, newJson, 'utf8');
  console.log(
    `[watch-manifest] manifest.json atualizado com ${newManifest.mosaics.length} mosaicos.`
  );
}

// Rodar uma atualização completa (usado no start e em cada mudança relevante)
function regenerateManifest() {
  try {
    const manifest = buildManifestFromFolder();
    writeManifestIfChanged(manifest);
  } catch (e) {
    console.error('[watch-manifest] Erro ao gerar manifest:', e);
  }
}

// ===============
// INÍCIO DO WATCH
// ===============
console.log('[watch-manifest] Observando pasta:', OUTPUT_DIR);
regenerateManifest(); // gera no start com o que já existe

// Observa criação/remoção de arquivos na pasta Output
const watcher = chokidar.watch(OUTPUT_DIR, {
  persistent: true,
  ignoreInitial: true,
  depth: 0
});

watcher
  .on('add', (filePath) => {
    console.log('[watch-manifest] Novo arquivo:', path.basename(filePath));
    regenerateManifest();
  })
  .on('unlink', (filePath) => {
    console.log('[watch-manifest] Arquivo removido:', path.basename(filePath));
    regenerateManifest();
  })
  .on('error', (error) => {
    console.error('[watch-manifest] Erro no watcher:', error);
  });
