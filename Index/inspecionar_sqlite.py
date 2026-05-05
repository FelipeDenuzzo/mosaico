import sqlite3
import os

DB_PATH = "/Users/felipedenuzzo/VSCODE/Mosaico Programas/Index/tiles_index.db"
LOG_PATH = "/Users/felipedenuzzo/VSCODE/Mosaico Programas/Index/debug_sqlite.log"

def main():
    if not os.path.exists(DB_PATH):
        with open(LOG_PATH, "w") as f:
            f.write(f"Banco de dados não encontrado em {DB_PATH}\n")
        print("Banco de dados não encontrado. Execute o indexador.py primeiro para criar o banco de dados.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    with open(LOG_PATH, "w", encoding="utf-8") as f:
        f.write(f"=== INSPECIONANDO BANCO DE DADOS ===\n")
        f.write(f"Caminho: {DB_PATH}\n\n")

        # 1. Tabelas
        f.write("--- TABELAS ---\n")
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        for name, sql in tables:
            f.write(f"Tabela: {name}\nSQL: {sql}\n\n")

        # 2. Indices
        f.write("--- INDICES ---\n")
        cursor.execute("SELECT name, tbl_name, sql FROM sqlite_master WHERE type='index'")
        indices = cursor.fetchall()
        for name, tbl_name, sql in indices:
            f.write(f"Indice: {name} (Tabela: {tbl_name})\nSQL: {sql}\n\n")

        # 3. Table Info para 'tiles'
        f.write("--- ESTRUTURA DA TABELA 'tiles' ---\n")
        cursor.execute("PRAGMA table_info(tiles)")
        columns = cursor.fetchall()
        if not columns:
            f.write("Tabela 'tiles' não encontrada.\n")
        else:
            for col in columns:
                # cid, name, type, notnull, dflt_value, pk
                f.write(f"Coluna {col[0]}: {col[1]} | Tipo: {col[2]} | PK: {col[5]}\n")
        f.write("\n")

        # 4 & 5. EXPLAIN QUERY PLAN
        f.write("--- EXPLAIN QUERY PLAN ---\n")
        queries = [
            "SELECT * FROM tiles WHERE categoria = 'Geral'",
            "SELECT * FROM tiles WHERE bucket = '10_10_10'",
            "SELECT * FROM tiles WHERE categoria = 'Geral' AND bucket = '10_10_10'",
            "SELECT * FROM tiles WHERE r > 100 AND g > 100"
        ]

        if columns:
            for q in queries:
                f.write(f"Query: {q}\n")
                try:
                    cursor.execute(f"EXPLAIN QUERY PLAN {q}")
                    plan = cursor.fetchall()
                    
                    uses_index = False
                    for step in plan:
                        detail = step[3]
                        f.write(f"  -> {detail}\n")
                        if "USING INDEX" in detail.upper():
                            uses_index = True

                    if uses_index:
                        f.write("Conclusão: usa índice (aparece USING INDEX/SEARCH ... USING INDEX)\n\n")
                    else:
                        f.write("Conclusão: não usa índice (aparece SCAN ...)\n\n")
                except Exception as e:
                    f.write(f"Erro ao executar EXPLAIN: {e}\n\n")

    conn.close()
    print("Inspeção concluída! Log gerado em debug_sqlite.log")

if __name__ == "__main__":
    main()