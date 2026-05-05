import os
import sqlite3

BASE_DIR = "/Users/felipedenuzzo/VSCODE/Mosaico Programas"
DB_PATH = os.path.join(BASE_DIR, "Index", "tiles_index.db")

# 1) Backup de segurança
BACKUP_PATH = DB_PATH + ".backup_before_fix"
if not os.path.exists(BACKUP_PATH):
    import shutil
    shutil.copy2(DB_PATH, BACKUP_PATH)
    print(f"Backup criado em: {BACKUP_PATH}")
else:
    print(f"Backup já existe em: {BACKUP_PATH}")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# 2) Conferir quantos registros têm caminho antigo
old_prefixes = [
    "/Users/felipedenuzzo/VSCODE/Mosaico Programas/Tiles/Informação/100_72",
    "/Users/felipedenuzzo/VSCODE/Mosaico Programas/Tiles/Informação/100_72",
    "/Users/felipedenuzzo/VSCODE/Mosaico Programas/Tiles/Medicamentos/TODAS_100_72",
    "/Users/felipedenuzzo/VSCODE/Mosaico Programas/Tiles/Pornografia/100_72",
]

print("\nAntes da correção:")
for p in old_prefixes:
    cur.execute("SELECT COUNT(*) FROM tiles WHERE path LIKE ?;", (p + "%",))
    qtd = cur.fetchone()[0]
    print(f"{p} -> {qtd} registros")

# 3) Atualizar substituindo prefixo antigo por acervo
NEW_PREFIX = "/Users/felipedenuzzo/VSCODE/Mosaico Programas/acervo"

total_atualizados = 0
for p in old_prefixes:
    cur.execute(
        "UPDATE tiles SET path = REPLACE(path, ?, ?) WHERE path LIKE ?;",
        (p, NEW_PREFIX, p + "%")
    )
    afetados = cur.rowcount
    total_atualizados += afetados
    print(f"Atualizados {afetados} registros de prefixo {p}")

conn.commit()

print(f"\nTotal de registros atualizados: {total_atualizados}")

# 4) Conferir depois
print("\nDepois da correção:")
for p in old_prefixes:
    cur.execute("SELECT COUNT(*) FROM tiles WHERE path LIKE ?;", (p + "%",))
    qtd = cur.fetchone()[0]
    print(f"{p} -> {qtd} registros")

import sqlite3, os

BASE_DIR = "/Users/felipedenuzzo/VSCODE/Mosaico Programas"
DB_PATH = os.path.join(BASE_DIR, "Index", "tiles_index.db")

conn = sqlite3.connect(DB_PATH)
conn.execute("VACUUM;")
conn.close()
print("VACUUM concluído.")

conn.close()
print("\nConcluído.")