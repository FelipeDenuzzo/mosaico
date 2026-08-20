import os
import sqlite3
import shutil

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "tiles_index.db")

print(f"Banco de dados alvo: {DB_PATH}")

# 1) Backup de segurança
BACKUP_PATH = DB_PATH + ".backup_reindex"
if not os.path.exists(BACKUP_PATH):
    shutil.copy2(DB_PATH, BACKUP_PATH)
    print(f"✓ Backup criado em: {BACKUP_PATH}")
else:
    print(f"✓ Backup já existe em: {BACKUP_PATH}")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# 2) Descobrir o prefixo antigo dinamicamente
cur.execute("SELECT path FROM tiles LIMIT 1;")
row = cur.fetchone()
if not row:
    print("Nenhum registro encontrado no banco de dados para reindexar.")
    conn.close()
    exit(0)

old_path = row[0]
if "/acervo" in old_path:
    old_prefix = old_path.split("/acervo")[0] + "/acervo"
elif "\\acervo" in old_path:
    old_prefix = old_path.split("\\acervo")[0] + "\\acervo"
else:
    print(f"Não foi possível identificar o padrão do acervo no caminho: {old_path}")
    conn.close()
    exit(1)

print(f"\nAmostra de caminho atual no banco: {old_path}")
print(f"Prefixo antigo identificado: {old_prefix}")
print(f"Prefixo novo a ser aplicado:  {NEW_PREFIX}")

if old_prefix == NEW_PREFIX:
    print("✓ O banco de dados já está com os caminhos corretos para esta máquina. Nenhuma alteração necessária.")
    conn.close()
    exit(0)

# 3) Substituir prefixos
cur.execute(
    "UPDATE tiles SET path = REPLACE(path, ?, ?) WHERE path LIKE ?;",
    (old_prefix, NEW_PREFIX, f"{old_prefix}%")
)
afetados = cur.rowcount
print(f"✓ Caminhos atualizados: {afetados} registros modificados.")

conn.commit()

# 4) Verificando depois
cur.execute("SELECT path FROM tiles LIMIT 3;")
rows = cur.fetchall()
print("\nAmostra de caminhos corrigidos:")
for r in rows:
    print(f"  {r[0]}")

# 5) Otimização
print("\nOtimizando o banco com VACUUM...")
conn.execute("VACUUM;")
conn.close()
print("✓ VACUUM concluído.")
print("\nProcesso de re-indexação finalizado com sucesso!")
