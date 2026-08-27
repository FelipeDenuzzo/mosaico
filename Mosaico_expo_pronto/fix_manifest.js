const fs = require('fs');
const path = './manifest.json';
const data = JSON.parse(fs.readFileSync(path));
if (!data.seen) data.seen = [];
data.seen.push(...data.queue.map(q => q.name));
data.queue = [];
fs.writeFileSync(path, JSON.stringify(data, null, 2));
console.log('Fila limpa!');
