# Este script recria o banco de dados do zero, indexando todas as imagens do acervo atual.
# Use apenas se quiser começar do zero, apagando o banco atual e criando um novo com os arquivos do acervo.
# Certifique-se de que o acervo esteja atualizado com as imagens que deseja indexar antes de rodar este script.
# cd "/Users/felipedenuzzo/VSCODE/Mosaico Programas/Index"
#  python3 indexar_acervo_do_zero.py
import sqlite3
from pathlib import Path

from indexador import (
    ensure_schema,
    upsert_tile,
    iter_image_files,
    PASTA_ACERVO_TILES,
    remover_tiles_orfaos,
)

DB_NAME = "tiles_index.db"


def main():
    base_dir = Path(__file__).resolve().parent
    db_path = base_dir / DB_NAME

    print(f"=== REINDEXANDO ACERVO DO ZERO ===")
    print(f"Banco novo: {db_path}")
    print(f"Acervo: {PASTA_ACERVO_TILES}\n")

    conn = sqlite3.connect(db_path)
    try:
        ensure_schema(conn)

        total = 0
        erros = 0

        for path in iter_image_files(PASTA_ACERVO_TILES):
            categoria = "Informacao"  # tudo como Informacao por enquanto
            try:
                upsert_tile(conn, categoria, path)
                total += 1
                if total % 200 == 0:
                    conn.commit()
                    print(f"{total} imagens processadas...")
            except Exception as e:
                erros += 1
                print(f"[ERRO] {path}: {e}")

        conn.commit()
        print(f"\nConcluído. {total} imagens indexadas, {erros} com erro.")

        # Limpa eventuais órfãos
        removidos = remover_tiles_orfaos(conn)
        print(f"Tiles órfãos removidos: {removidos}")

        print(f"Banco criado em: {db_path}")
        print("=== FIM REINDEXACAO ===")
    finally:
        conn.close()


if __name__ == "__main__":
    main()