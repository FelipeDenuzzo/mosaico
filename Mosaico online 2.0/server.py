#!/usr/bin/env python3
"""
Servidor HTTP & API Backend para Mosaico Online 2.0
Gerencia a câmera WebRTC, controle de IP (1 foto/24h),
indexação de tiles, geração de mosaicos e fila FIFO (máx 10 mosaicos).
"""

import os
import sys
import json
import time
import sqlite3
import base64
import http.server
import socketserver
from io import BytesIO
from datetime import datetime, timezone
from PIL import Image

# Garantir imports locais
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from mosaico import criar_mosaico, invalidar_cache_catalogo, INDEX_DB_PATH
from indexador import upsert_tile, ensure_schema, DB_NAME

PORT = 8000
ACERVO_DIR = os.path.join(BASE_DIR, "acervo")
OUTPUT_DIR = os.path.join(BASE_DIR, "Output")
INPUT_DIR = os.path.join(BASE_DIR, "input")
QUEUE_FILE = os.path.join(BASE_DIR, "queue.json")
IP_DB_FILE = os.path.join(BASE_DIR, "ip_rate_limit.db")

# Assegurar pastas existem
for folder in [ACERVO_DIR, OUTPUT_DIR, INPUT_DIR]:
    os.makedirs(folder, exist_ok=True)

# Assegurar banco de tiles inicializado
conn_init = sqlite3.connect(os.path.join(BASE_DIR, DB_NAME))
ensure_schema(conn_init)
conn_init.close()

# Inicializar banco de rate limit de IP
def init_ip_db():
    conn = sqlite3.connect(IP_DB_FILE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ip_logs (
            ip TEXT PRIMARY KEY,
            last_upload INTEGER NOT NULL
        )
    """)
    conn.commit()
    conn.close()

init_ip_db()

def get_client_ip(handler):
    """Extrai o IP do cliente considerando headers de proxy (Hostinger/GCP)."""
    forwarded = handler.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return handler.client_address[0]

def check_ip_rate_limit(ip: str):
    """Verifica se o IP pode enviar foto (1 envio a cada 24 horas). Libera em dev/localhost."""
    if ip in ("127.0.0.1", "localhost", "::1") or os.getenv("DISABLE_IP_LIMIT", "1") == "1":
        return True, 0, "OK"

    conn = sqlite3.connect(IP_DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT last_upload FROM ip_logs WHERE ip = ?", (ip,))
    row = cursor.fetchone()
    conn.close()

    now = int(time.time())
    if not row:
        return True, 0, "OK"
    
    last_upload = row[0]
    elapsed = now - last_upload
    seconds_in_day = 86400

    if elapsed >= seconds_in_day:
        return True, 0, "OK"
    else:
        remaining = seconds_in_day - elapsed
        hours = remaining // 3600
        minutes = (remaining % 3600) // 60
        return False, remaining, f"{hours}h {minutes}m"

def record_ip_upload(ip: str):
    """Registra o timestamp do upload para o IP."""
    conn = sqlite3.connect(IP_DB_FILE)
    now = int(time.time())
    conn.execute("""
        INSERT INTO ip_logs (ip, last_upload) VALUES (?, ?)
        ON CONFLICT(ip) DO UPDATE SET last_upload = excluded.last_upload
    """, (ip, now))
    conn.commit()
    conn.close()

def load_queue():
    if not os.path.exists(QUEUE_FILE):
        return []
    try:
        with open(QUEUE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save_queue(queue):
    with open(QUEUE_FILE, "w", encoding="utf-8") as f:
        json.dump(queue, f, ensure_ascii=False, indent=2)

class MosaicoRequestHandler(http.server.SimpleHTTPRequestHandler):
    extensions_map = {
        **http.server.SimpleHTTPRequestHandler.extensions_map,
        ".avif": "image/avif",
        ".webp": "image/webp",
        ".json": "application/json",
    }

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        if self.path.startswith("/api/queue"):
            ip = get_client_ip(self)
            allowed, remaining, remaining_str = check_ip_rate_limit(ip)
            queue = load_queue()

            response = {
                "can_upload": allowed,
                "remaining_seconds": remaining,
                "remaining_formatted": remaining_str,
                "queue": queue
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps(response, ensure_ascii=False).encode("utf-8"))
            return

        super().do_GET()

    def do_POST(self):
        if self.path == "/api/upload":
            ip = get_client_ip(self)
            allowed, remaining, remaining_str = check_ip_rate_limit(ip)

            if not allowed:
                self.send_response(429)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                res = {
                    "error": f"Você já enviou uma foto hoje. Aguarde {remaining_str} para enviar outra.",
                    "remaining_seconds": remaining
                }
                self.wfile.write(json.dumps(res, ensure_ascii=False).encode("utf-8"))
                return

            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            try:
                data = json.loads(body.decode("utf-8"))
                image_data = data.get("image")
                if not image_data or not image_data.startswith("data:image"):
                    raise ValueError("Formato de imagem inválido")

                # Decodificar Base64
                header, encoded = image_data.split(",", 1)
                img_bytes = base64.b64decode(encoded)
                img = Image.open(BytesIO(img_bytes)).convert("RGB")

                # Recorte quadrado 400x400 para salvar como tile no acervo
                w, h = img.size
                side = min(w, h)
                left = (w - side) // 2
                top = (h - side) // 2
                img_cropped = img.crop((left, top, left + side, top + side))
                img_tile = img_cropped.resize((400, 400), Image.Resampling.LANCZOS)

                timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
                serial = int(time.time() * 1000) % 1000000

                # 1. Salvar no acervo permanente
                tile_filename = f"tile_{timestamp_str}_{serial:06d}.jpg"
                tile_path = os.path.join(ACERVO_DIR, tile_filename)
                img_tile.save(tile_path, "JPEG", quality=90)

                # 2. Indexar tile no SQLite e invalidar cache do mosaico
                conn = sqlite3.connect(os.path.join(BASE_DIR, DB_NAME))
                upsert_tile(conn, "Geral", tile_path)
                conn.close()
                invalidar_cache_catalogo()

                # 3. Salvar imagem base temporária para o gerador de mosaico (mínimo 1200px)
                base_size = max(1200, side)
                img_base = img_cropped.resize((base_size, base_size), Image.Resampling.LANCZOS)
                base_input_path = os.path.join(INPUT_DIR, f"base_{timestamp_str}.jpg")
                img_base.save(base_input_path, "JPEG", quality=95)

                # 4. Criar mosaico
                output_filename = f"mosaico_{timestamp_str}_{serial:06d}.webp"
                output_path = os.path.join(OUTPUT_DIR, output_filename)

                # Executar motor de mosaico
                criar_mosaico(caminho_base=base_input_path, caminho_saida=output_path)

                # Gerar thumbnail leve para a barra da fila (300px)
                thumb_filename = f"thumb_{timestamp_str}_{serial:06d}.webp"
                thumb_path = os.path.join(OUTPUT_DIR, thumb_filename)
                with Image.open(output_path) as out_img:
                    out_img.resize((300, 300), Image.Resampling.LANCZOS).save(thumb_path, "WEBP", quality=80)

                # Limpar arquivo temporário de input
                if os.path.exists(base_input_path):
                    os.remove(base_input_path)

                # Registrar limite do IP
                record_ip_upload(ip)

                # 5. Gerenciar a Fila (FIFO Max 10 Mosaicos)
                queue = load_queue()
                new_item = {
                    "id": f"mosaico_{serial:06d}",
                    "title": f"Mosaico #{len(queue) + 1}",
                    "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M"),
                    "url": f"/Output/{output_filename}",
                    "thumb_url": f"/Output/{thumb_filename}"
                }
                queue.append(new_item)

                # Se excede 10 mosaicos na fila: apaga o mosaico mais antigo da exibição
                # NOTA: O tile original em acervo/ PERMANECE intocado!
                while len(queue) > 10:
                    old_item = queue.pop(0)
                    old_output_path = os.path.join(BASE_DIR, old_item["url"].lstrip("/"))
                    old_thumb_path = os.path.join(BASE_DIR, old_item["thumb_url"].lstrip("/"))

                    if os.path.exists(old_output_path):
                        try:
                            os.remove(old_output_path)
                        except Exception as e:
                            print(f"Aviso ao remover mosaico antigo: {e}")
                    if os.path.exists(old_thumb_path):
                        try:
                            os.remove(old_thumb_path)
                        except Exception as e:
                            print(f"Aviso ao remover thumb antigo: {e}")

                save_queue(queue)

                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                res = {
                    "success": True,
                    "new_item": new_item,
                    "queue": queue
                }
                self.wfile.write(json.dumps(res, ensure_ascii=False).encode("utf-8"))

            except Exception as e:
                import traceback
                traceback.print_exc()
                self.send_response(500)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                res = {"error": f"Erro interno ao processar mosaico: {str(e)}"}
                self.wfile.write(json.dumps(res, ensure_ascii=False).encode("utf-8"))
            return

        super().do_POST()

class ReuseAddrTCPServer(socketserver.TCPServer):
    allow_reuse_address = True

if __name__ == "__main__":
    os.chdir(BASE_DIR)
    print(f"==================================================")
    print(f"🚀 Mosaico Online 2.0 Servidor HTTP & API")
    print(f"📍 Rodando em: http://localhost:{PORT}/")
    print(f"==================================================")
    with ReuseAddrTCPServer(("", PORT), MosaicoRequestHandler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n✓ Servidor finalizado com sucesso.")
