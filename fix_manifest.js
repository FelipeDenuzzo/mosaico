const fs = require('fs');
const path = '/Users/felipedenuzzo/VSCODE/Mosaico Programas/Mosaico_exibicao/manifest.json';
const data = JSON.parse(fs.readFileSync(path));
if (!data.seen) data.seen = [];
data.seen.push(...data.queue.map(q => q.name));
data.queue = [];
fs.writeFileSync(path, JSON.stringify(data, null, 2));
const site_path = '/Users/felipedenuzzo/VSCODE/Mosaico Programas/Site/Mosaico_exibicao/manifest.json';
if (fs.existsSync(site_path)) {
  fs.writeFileSync(site_path, JSON.stringify(data, null, 2));
}
console.log('Fila limpa!');
