#!/usr/bin/env python3
"""
Servidor HTTP customizado com suporte a MIME type AVIF.
Use em vez de `python -m http.server 8000`
para garantir que arquivos .avif sejam servidos com Content-Type correto.
"""
import http.server
import socketserver
import os
from datetime import datetime

PORT = 8000
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOGS_DIR, "serve_site.log")

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

class Handler(http.server.SimpleHTTPRequestHandler):
    """Handler que inclui MIME type para AVIF e outros formatos modernos."""
    extensions_map = {
        **http.server.SimpleHTTPRequestHandler.extensions_map,
        ".avif": "image/avif",
        ".webp": "image/webp",
    }

    def log_message(self, format, *args):
        # Registra no log do serve_site
        try:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            msg = f"[{ts}] {self.address_string()} - {format % args}\n"
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(msg)
        except Exception:
            pass

    def end_headers(self):
        """Adiciona headers de cache-control para melhor performance."""
        self.send_header("Cache-Control", "public, max-age=3600")
        super().end_headers()


class ReuseAddrTCPServer(socketserver.TCPServer):
    allow_reuse_address = True

if __name__ == "__main__":
    os.chdir(BASE_DIR)
    with ReuseAddrTCPServer(("", PORT), Handler) as httpd:
        log(f"Servidor HTTP rodando em http://localhost:{PORT}/ (raiz: {BASE_DIR})")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            log("Servidor HTTP encerrado")
