#!/usr/bin/env python3
"""
Servidor HTTP customizado com suporte a MIME type AVIF.
Use em vez de `python -m http.server 8000`
para garantir que arquivos .avif sejam servidos com Content-Type correto.
"""
import http.server
import socketserver
import os

PORT = 8000

class Handler(http.server.SimpleHTTPRequestHandler):
    """Handler que inclui MIME type para AVIF e outros formatos modernos."""
    extensions_map = {
        **http.server.SimpleHTTPRequestHandler.extensions_map,
        ".avif": "image/avif",
        ".webp": "image/webp",
    }

    def end_headers(self):
        """Adiciona headers de cache-control para melhor performance."""
        self.send_header("Cache-Control", "public, max-age=3600")
        super().end_headers()


class ReuseAddrTCPServer(socketserver.TCPServer):
    allow_reuse_address = True

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    site_dir = os.path.join(base_dir, "Site")
    os.chdir(site_dir)
    with ReuseAddrTCPServer(("", PORT), Handler) as httpd:
        print(f"✓ Servidor HTTP rodando em http://localhost:{PORT}/")
        print(f"  Servindo arquivos de: {site_dir}")
        print(f"  Suporte para: JPG, PNG, WebP, AVIF")
        print(f"  Pressione Ctrl+C para parar")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n✓ Servidor encerrado")
