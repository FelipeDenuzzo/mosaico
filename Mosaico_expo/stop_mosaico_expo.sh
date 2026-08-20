#!/usr/bin/env bash

BASE_DIR=$(cd "$(dirname "$0")" && pwd)
PID_FILE="$BASE_DIR/.mosaico_expo_pids"

cd "$BASE_DIR" || exit 1

echo "Encerrando serviços Mosaico EXPO..."

# 1) Matar processos listados no arquivo
if [ -f "$PID_FILE" ]; then
  while read -r pid; do
    if [ -n "$pid" ]; then
      kill -9 "$pid" 2>/dev/null
      echo "✓ Processo $pid encerrado"
    fi
  done < "$PID_FILE"
  rm -f "$PID_FILE"
  echo "✓ Arquivo de PIDs removido"
else
  echo "⚠ Arquivo .mosaico_expo_pids não encontrado"
fi

pkill -f "auto_mosaic.py" 2>/dev/null && echo "✓ auto_mosaic.py encerrado"
pkill -f "serve_site.py" 2>/dev/null && echo "✓ serve_site.py encerrado"
pkill -f "indexador.py" 2>/dev/null && echo "✓ indexador.py encerrado"
pkill -f "watch-manifest.js" 2>/dev/null && echo "✓ watch-manifest.js encerrado"
pkill -f "captura.py" 2>/dev/null && echo "✓ captura.py (câmera) encerrado"

# 3) Limpar portas ocupadas (8000 e 8081)
sleep 1
lsof -i :8000 -i :8081 2>/dev/null | tail -n +2 | awk '{print $2}' | sort -u | xargs kill -9 2>/dev/null
echo "✓ Portas limpas"

echo "✓ Sistema Mosaico EXPO encerrado completamente"
