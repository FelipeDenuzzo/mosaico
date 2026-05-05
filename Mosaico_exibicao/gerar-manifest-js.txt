const fs = require('fs');
const path = require('path');

const projectRoot = process.cwd();
const outputDir = path.resolve(projectRoot, '../Output');
const manifestPath = path.resolve(projectRoot, 'manifest.json');
const allowed = new Set(['.jpg', '.jpeg', '.png', '.webp', '.avif']);

if (!fs.existsSync(outputDir)) {
  console.error(`Pasta Output não encontrada: ${outputDir}`);
  process.exit(1);
}

const files = fs.readdirSync(outputDir)
  .filter(name => allowed.has(path.extname(name).toLowerCase()))
  .map(name => {
    const fullPath = path.join(outputDir, name);
    const stats = fs.statSync(fullPath);
    return {
      name,
      file: `../Output/${name}`,
      createdAt: stats.mtime.toISOString(),
      size: stats.size
    };
  })
  .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

const manifest = {
  generatedAt: new Date().toISOString(),
  source: outputDir,
  total: files.length,
  mosaics: files
};

fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2), 'utf8');
console.log(`manifest.json criado com ${files.length} mosaicos.`);