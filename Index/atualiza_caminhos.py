import sqlite3

PASTA_ACERVO_TILES = "/Users/felipedenuzzo/VSCODE/Mosaico Programas/acervo"

# Substituições de caminhos antigos para os novos
substituicoes = [
    ("/Volumes/2024 TRAB/2025/ART/Mosaico/Pixel/Informação/Mosaico - Informação - 200 x 200 px", PASTA_ACERVO_TILES),
    ("/Volumes/2024 TRAB/2025/ART/Mosaico/Pixel/Medicamentos/Todas", PASTA_ACERVO_TILES),
    ("/Users/felipedenuzzo/VSCODE/Mosaico Programas/Pornografia", PASTA_ACERVO_TILES),
]

db_path = "/Users/felipedenuzzo/VSCODE/Mosaico Programas/Index/tiles_index.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

for antigo, novo in substituicoes:
    print(f"Atualizando caminhos: {antigo} -> {novo}")
    cur.execute(
        "UPDATE tiles SET path = REPLACE(path, ?, ?) WHERE path LIKE ?",
        (antigo, novo, f"{antigo}%")
    )

conn.commit()
conn.close()
print("Atualização concluída!")