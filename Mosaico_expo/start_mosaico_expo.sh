#!/usr/bin/env bash

BASE_DIR=$(cd "$(dirname "$0")" && pwd)

cd "$BASE_DIR" || exit 1

# Ativa o ambiente virtual principal da pasta mãe ou o local
if [ -f "$BASE_DIR/../.venv/bin/activate" ]; then
  source "$BASE_DIR/../.venv/bin/activate"
elif [ -f "$BASE_DIR/.venv/bin/activate" ]; then
  source "$BASE_DIR/.venv/bin/activate"
else
  echo "Erro: Arquivo .venv/bin/activate não encontrado em $BASE_DIR ou $BASE_DIR/../"
  exit 1
fi

# Limpar arquivo de PIDs anterior
PID_FILE="$BASE_DIR/.mosaico_expo_pids"
rm -f "$PID_FILE"

# 1) Servidor HTTP estático (mural) com suporte a AVIF (porta 8000)
echo "Iniciando servidor HTTP em http://localhost:8000 ..."
python -u serve_site.py > serve_site.log 2>&1 &
PID_HTTP=$!

# 2) Watcher de mosaicos (auto_mosaic)
echo "Iniciando watcher auto_mosaic ..."
python -u auto_mosaic.py > auto_mosaic_stdout.log 2>&1 &
PID_WATCHER=$!

# 3) Indexador de tiles
echo "Iniciando indexador ..."
python -u indexador.py > indexador.log 2>&1 &
PID_INDEX=$!

# 4) Watcher de manifest (watch-manifest.js) na porta 8081
echo "Iniciando watch-manifest.js ..."
node watch-manifest.js > watch-manifest.log 2>&1 &
PID_MANIFEST=$!

# 5) Captura da câmera do iMac em loop
echo "Iniciando captura da câmera (webcam) ..."
python -u camera/captura.py > camera.log 2>&1 &
PID_CAMERA=$!

# Salvar PIDs em arquivo para fechamento depois
echo "$PID_HTTP" >> "$PID_FILE"
echo "$PID_WATCHER" >> "$PID_FILE"
echo "$PID_INDEX" >> "$PID_FILE"
echo "$PID_MANIFEST" >> "$PID_FILE"
echo "$PID_CAMERA" >> "$PID_FILE"

echo
echo "Processos iniciados para EXPO (redirecionados para logs):"
echo "HTTP Server PID (8000):   $PID_HTTP"
echo "auto_mosaic PID:          $PID_WATCHER"
echo "indexador PID:            $PID_INDEX"
echo "watch-manifest PID (881): $PID_MANIFEST"
echo "camera PID:               $PID_CAMERA"
echo
echo "Use 'bash stop_mosaico_expo.sh' para encerrar tudo."
