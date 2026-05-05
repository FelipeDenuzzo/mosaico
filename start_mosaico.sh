#!/usr/bin/env bash

BASE_DIR="/Users/felipedenuzzo/VSCODE/Mosaico Programas"

cd "$BASE_DIR" || exit 1

# Ativa o ambiente virtual
if [ -f "$BASE_DIR/.venv/bin/activate" ]; then
  source "$BASE_DIR/.venv/bin/activate"
else
  echo "Arquivo .venv/bin/activate não encontrado em $BASE_DIR"
  exit 1
fi

# Limpar arquivo de PIDs anterior
rm -f "$BASE_DIR/.mosaico_pids"

# 1) Flask Server (upload e status)
echo "Iniciando Flask server em http://localhost:5000 ..."
cd "$BASE_DIR/Site" || exit 1
python server.py &
PID_FLASK=$!

# 2) Servidor HTTP estático (site) com suporte a AVIF
echo "Iniciando servidor HTTP em http://localhost:8000 ..."
cd "$BASE_DIR" || exit 1
python serve_site.py &
PID_HTTP=$!

# 2) Watcher de mosaicos (auto_mosaic)
echo "Iniciando watcher auto_mosaic ..."
cd "$BASE_DIR/mosaic_creator" || exit 1
python auto_mosaic.py &
PID_WATCHER=$!

# 3) Indexador
echo "Iniciando indexador ..."
cd "$BASE_DIR/Index" || exit 1
python indexador.py &
PID_INDEX=$!

# 4) Watcher de manifest (watch-manifest.js)
echo "Iniciando watch-manifest.js ..."
cd "$BASE_DIR/mosaic_creator" || exit 1
node watch-manifest.js &
PID_MANIFEST=$!

# Salvar PIDs em arquivo para fechamento depois
echo "$PID_FLASK" >> "$BASE_DIR/.mosaico_pids"
echo "$PID_HTTP" >> "$BASE_DIR/.mosaico_pids"
echo "$PID_WATCHER" >> "$BASE_DIR/.mosaico_pids"
echo "$PID_INDEX" >> "$BASE_DIR/.mosaico_pids"
echo "$PID_MANIFEST" >> "$BASE_DIR/.mosaico_pids"

echo
echo "Processos iniciados:"
echo "Flask Server PID:       $PID_FLASK"
echo "HTTP Server PID:        $PID_HTTP"
echo "auto_mosaic PID:        $PID_WATCHER"
echo "indexador PID:          $PID_INDEX"
echo "watch-manifest.js PID:  $PID_MANIFEST"
echo
echo "Use 'bash stop_mosaico.sh' para encerrar tudo."