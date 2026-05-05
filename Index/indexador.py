import os
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable, Tuple

from PIL import Image


# ==============================================================================
# CONFIGURAÇÃO DE PASTAS
# ==============================================================================

# Pasta temporária de entrada (funil de novos tiles, ex: cópia do UX feita pelo watcher)
PASTA_TEMPORARIA_TILES = "/Users/felipedenuzzo/VSCODE/Mosaico Programas/archive"

# Pasta do acervo final de tiles 100x100 usados pelo motor de mosaico
PASTA_ACERVO_TILES = "/Users/felipedenuzzo/VSCODE/Mosaico Programas/acervo"


# ==============================================================================
# CONFIGURAÇÃO DE AGENDAMENTO
# ==============================================================================

MODO_TESTE = True  # True = laço em minutos, False = janela noturna diária

# --- Modo Teste ---
TESTE_INTERVALO_MINUTOS = 1
RODAR_IMEDIATO_NO_START = True

# --- Modo Produção (Noturno) ---
PROD_HORA = 2
PROD_MINUTO = 30


# ==============================================================================
# CONFIGURAÇÃO DO BANCO E INDEXAÇÃO
# ==============================================================================

DB_NAME = "tiles_index.db"
SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}
BUCKET_DIVISOR = 16
LOG_DETALHADO_INDEXACAO = True

# Categorias lógicas; caminho físico é sempre PASTA_ACERVO_TILES
DEFAULT_CATEGORIES = {
    "Informacao": "Informacao",
    "Medicamentos": "Medicamentos",
    "Pornografia": "Pornografia",
    "Geral": "Geral",
}


# ==============================================================================
# FUNÇÕES DE COR / DESCRITORES
# ==============================================================================

def average_color(image: Image.Image) -> Tuple[int, int, int]:
    small = image.convert("RGB").resize((1, 1), Image.Resampling.BOX)
    return small.getpixel((0, 0))


def region_average(image: Image.Image, box: Tuple[int, int, int, int]) -> Tuple[int, int, int]:
    region = image.crop(box)
    return average_color(region)


def compute_descriptors(path: str):
    with Image.open(path) as img:
        rgb = img.convert("RGB")
        width, height = rgb.size
        r, g, b = average_color(rgb)

        x1 = width // 3
        x2 = (width * 2) // 3
        y1 = height // 3
        y2 = (height * 2) // 3

        v1 = region_average(rgb, (0, 0, x1, height))
        v2 = region_average(rgb, (x1, 0, x2, height))
        v3 = region_average(rgb, (x2, 0, width, height))

        h1 = region_average(rgb, (0, 0, width, y1))
        h2 = region_average(rgb, (0, y1, width, y2))
        h3 = region_average(rgb, (0, y2, width, height))

        return {
            "width": width,
            "height": height,
            "r": r,
            "g": g,
            "b": b,
            "bucket": f"{r // BUCKET_DIVISOR}_{g // BUCKET_DIVISOR}_{b // BUCKET_DIVISOR}",
            "v1_r": v1[0], "v1_g": v1[1], "v1_b": v1[2],
            "v2_r": v2[0], "v2_g": v2[1], "v2_b": v2[2],
            "v3_r": v3[0], "v3_g": v3[1], "v3_b": v3[2],
            "h1_r": h1[0], "h1_g": h1[1], "h1_b": h1[2],
            "h2_r": h2[0], "h2_g": h2[1], "h2_b": h2[2],
            "h3_r": h3[0], "h3_g": h3[1], "h3_b": h3[2],
        }


def log_indexacao_detalhada(categoria: str, path: str, data: dict) -> None:
    if not LOG_DETALHADO_INDEXACAO:
        return

    print(f"[INDEXADO] arquivo={os.path.basename(path)} categoria={categoria}")
    print(
        f"  - tamanho={data['width']}x{data['height']} "
        f"media_rgb=({data['r']}, {data['g']}, {data['b']}) bucket={data['bucket']}"
    )
    print(
        "  - areas_verticais: "
        f"v1=({data['v1_r']}, {data['v1_g']}, {data['v1_b']}) "
        f"v2=({data['v2_r']}, {data['v2_g']}, {data['v2_b']}) "
        f"v3=({data['v3_r']}, {data['v3_g']}, {data['v3_b']})"
    )
    print(
        "  - areas_horizontais: "
        f"h1=({data['h1_r']}, {data['h1_g']}, {data['h1_b']}) "
        f"h2=({data['h2_r']}, {data['h2_g']}, {data['h2_b']}) "
        f"h3=({data['h3_r']}, {data['h3_g']}, {data['h3_b']})"
    )


# ==============================================================================
# ARQUIVOS / BANCO
# ==============================================================================

def iter_image_files(folder: str) -> Iterable[str]:
    for root, _, files in os.walk(folder):
        for name in files:
            if Path(name).suffix.lower() in SUPPORTED_EXTENSIONS:
                yield os.path.join(root, name)


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            categoria TEXT NOT NULL,
            path TEXT NOT NULL UNIQUE,
            filename TEXT NOT NULL,
            r INTEGER NOT NULL,
            g INTEGER NOT NULL,
            b INTEGER NOT NULL,
            bucket TEXT NOT NULL,
            width INTEGER NOT NULL,
            height INTEGER NOT NULL,
            v1_r INTEGER NOT NULL,
            v1_g INTEGER NOT NULL,
            v1_b INTEGER NOT NULL,
            v2_r INTEGER NOT NULL,
            v2_g INTEGER NOT NULL,
            v2_b INTEGER NOT NULL,
            v3_r INTEGER NOT NULL,
            v3_g INTEGER NOT NULL,
            v3_b INTEGER NOT NULL,
            h1_r INTEGER NOT NULL,
            h1_g INTEGER NOT NULL,
            h1_b INTEGER NOT NULL,
            h2_r INTEGER NOT NULL,
            h2_g INTEGER NOT NULL,
            h2_b INTEGER NOT NULL,
            h3_r INTEGER NOT NULL,
            h3_g INTEGER NOT NULL,
            h3_b INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tiles_categoria ON tiles(categoria)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tiles_bucket ON tiles(bucket)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tiles_categoria_bucket ON tiles(categoria, bucket)")
    conn.commit()


def upsert_tile(conn: sqlite3.Connection, categoria: str, path: str) -> None:
    data = compute_descriptors(path)
    now = datetime.now(UTC).isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO tiles (
            categoria, path, filename, r, g, b, bucket, width, height,
            v1_r, v1_g, v1_b, v2_r, v2_g, v2_b, v3_r, v3_g, v3_b,
            h1_r, h1_g, h1_b, h2_r, h2_g, h2_b, h3_r, h3_g, h3_b,
            created_at, updated_at
        ) VALUES (
            :categoria, :path, :filename, :r, :g, :b, :bucket, :width, :height,
            :v1_r, :v1_g, :v1_b, :v2_r, :v2_g, :v2_b, :v3_r, :v3_g, :v3_b,
            :h1_r, :h1_g, :h1_b, :h2_r, :h2_g, :h2_b, :h3_r, :h3_g, :h3_b,
            :created_at, :updated_at
        )
        ON CONFLICT(path) DO UPDATE SET
            categoria=excluded.categoria,
            filename=excluded.filename,
            r=excluded.r,
            g=excluded.g,
            b=excluded.b,
            bucket=excluded.bucket,
            width=excluded.width,
            height=excluded.height,
            v1_r=excluded.v1_r,
            v1_g=excluded.v1_g,
            v1_b=excluded.v1_b,
            v2_r=excluded.v2_r,
            v2_g=excluded.v2_g,
            v2_b=excluded.v2_b,
            v3_r=excluded.v3_r,
            v3_g=excluded.v3_g,
            v3_b=excluded.v3_b,
            h1_r=excluded.h1_r,
            h1_g=excluded.h1_g,
            h1_b=excluded.h1_b,
            h2_r=excluded.h2_r,
            h2_g=excluded.h2_g,
            h2_b=excluded.h2_b,
            h3_r=excluded.h3_r,
            h3_g=excluded.h3_g,
            h3_b=excluded.h3_b,
            updated_at=excluded.updated_at
        """,
        {
            "categoria": categoria,
            "path": path,
            "filename": os.path.basename(path),
            "created_at": now,
            "updated_at": now,
            **data,
        },
    )
    log_indexacao_detalhada(categoria, path, data)


# ==============================================================================
# FUNIL: ARCHIVE -> ACERVO (100x100) -> DB
# ==============================================================================

def inferir_categoria_temporaria(caminho_arquivo: str) -> str:
    rel_path = Path(caminho_arquivo).resolve().relative_to(Path(PASTA_TEMPORARIA_TILES).resolve())
    if len(rel_path.parts) > 1:
        return rel_path.parts[0]
    return "Geral"


def processar_pasta_temporaria(conn: sqlite3.Connection) -> int:
    """
    Processa imagens de archive, gera tiles 100x100 em acervo,
    indexa no tiles_index.db e apaga o original de archive após sucesso. [file:1]
    """
    if Path(PASTA_TEMPORARIA_TILES).resolve() == Path(PASTA_ACERVO_TILES).resolve():
        raise ValueError("PASTA_TEMPORARIA_TILES e PASTA_ACERVO_TILES nao podem ser a mesma pasta.")

    os.makedirs(PASTA_TEMPORARIA_TILES, exist_ok=True)
    os.makedirs(PASTA_ACERVO_TILES, exist_ok=True)

    arquivos = list(iter_image_files(PASTA_TEMPORARIA_TILES))
    if not arquivos:
        return 0

    print(f"Indexando {len(arquivos)} arquivos da pasta temporaria...")
    total_processados = 0

    for caminho_tmp in arquivos:
        nome_arquivo = os.path.basename(caminho_tmp)
        categoria = inferir_categoria_temporaria(caminho_tmp)

        try:
            if LOG_DETALHADO_INDEXACAO:
                print(f"[PROCESSO] iniciando arquivo={nome_arquivo} categoria={categoria}")

            # 1. Abrir imagem original de archive (alta)
            with Image.open(caminho_tmp) as img:
                img = img.convert("RGB")
                if LOG_DETALHADO_INDEXACAO:
                    print(f"  - original={img.width}x{img.height} modo=RGB")

                # 2. Recorte central quadrado
                lado = min(img.width, img.height)
                left = (img.width - lado) // 2
                top = (img.height - lado) // 2
                img_cropped = img.crop((left, top, left + lado, top + lado))
                if LOG_DETALHADO_INDEXACAO:
                    print(
                        f"  - crop_central={lado}x{lado} "
                        f"box=({left},{top},{left + lado},{top + lado})"
                    )

                # 3. Redimensionar para 100x100 (padrão do acervo)
                img_final = img_cropped.resize((100, 100), Image.Resampling.LANCZOS)
                if LOG_DETALHADO_INDEXACAO:
                    print("  - resize=100x100")

                # 4. Salvar no acervo com nome único
                timestamp = int(time.time() * 1000)
                nome_final = f"tile_{timestamp}_{nome_arquivo}"
                caminho_final_tmp = os.path.join(PASTA_ACERVO_TILES, f"._tmp_{nome_final}")
                caminho_final = os.path.join(PASTA_ACERVO_TILES, nome_final)

                img_final.save(caminho_final_tmp, "JPEG", quality=95)
                # rename atômico para evitar ler arquivo "meio escrito"
                os.replace(caminho_final_tmp, caminho_final)

                if LOG_DETALHADO_INDEXACAO:
                    print(f"  - salvo_em={caminho_final}")

            # 5. Indexar tile já pronto no acervo
            upsert_tile(conn, categoria, caminho_final)

            # 6. Apagar original de archive só depois de indexar
            os.remove(caminho_tmp)
            total_processados += 1

        except Exception as e:
            print(f"[ERRO] Falha ao processar tile temporario {nome_arquivo}: {e}")

    # Limpar JSONs auxiliares na pasta archive (se existirem)
    for nome in os.listdir(PASTA_TEMPORARIA_TILES):
        if nome.lower().endswith(".json"):
            try:
                os.remove(os.path.join(PASTA_TEMPORARIA_TILES, nome))
            except Exception as e:
                print(f"[ERRO] Falha ao remover JSON temporário {nome}: {e}")

    conn.commit()
    return total_processados


# ==============================================================================
# SINCRONIZAR BANCO COM ACERVO (REMOVER ÓRFÃOS)
# ==============================================================================

def remover_tiles_orfaos(conn: sqlite3.Connection) -> int:
    """
    Remove do banco todos os tiles cujo path não existe mais no filesystem.
    Retorna o número de registros removidos.
    """
    cur = conn.execute("SELECT id, path FROM tiles")
    rows = cur.fetchall()

    removidos = 0
    for tile_id, path in rows:
        if not os.path.exists(path):
            conn.execute("DELETE FROM tiles WHERE id = ?", (tile_id,))
            removidos += 1

    conn.commit()
    if removidos > 0:
        print(f"[INDEXADOR] Removidos {removidos} tiles órfãos (arquivos ausentes).")
    return removidos


# ==============================================================================
# ROTINA PRINCIPAL
# ==============================================================================

def executar_rotina() -> None:
    horas_agora = datetime.now().strftime("%H:%M:%S")
    print(f"\n[{horas_agora}] === Indexacao Iniciada ===")

    base_dir = Path(__file__).resolve().parent
    db_path = base_dir / DB_NAME

    conn = sqlite3.connect(db_path)
    try:
        ensure_schema(conn)

        print("\nVerificando tiles temporarios (archive)...")
        temporarios = processar_pasta_temporaria(conn)
        if temporarios > 0:
            print(f"Indexacao concluida. {temporarios} arquivos processados e limpos da pasta temporaria.")
        else:
            print("Nenhum tile novo na pasta temporaria.")

        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Rotina concluida.")
        print(f"Banco atualizado em: {db_path}")
        print("=== Indexacao Concluida ===\n")
    except Exception as exc:
        print(f"\n[ERRO] Falha fatal no indexador: {exc}\n")
    finally:
        conn.close()


def proxima_execucao_prod() -> datetime:
    agora = datetime.now()
    agendado = agora.replace(hour=PROD_HORA, minute=PROD_MINUTO, second=0, microsecond=0)
    if agendado <= agora:
        agendado += timedelta(days=1)
    return agendado


def main() -> None:
    print(f"\n{'='*50}")
    print("SERVICO INDEXADOR - STARTED")
    print(f"MODO_TESTE: {MODO_TESTE}")
    print(f"Aguardando novos tiles em: {PASTA_TEMPORARIA_TILES}")
    print(f"{'='*50}")

    if MODO_TESTE and RODAR_IMEDIATO_NO_START:
        print("Rodando imediato (Teste -> RODAR_IMEDIATO_NO_START = True)")
        executar_rotina()

    while True:
        agora = datetime.now()

        if MODO_TESTE:
            proximo = agora + timedelta(minutes=TESTE_INTERVALO_MINUTOS)
            print(f"Modo Teste -> Proximo horario agendado para: {proximo.strftime('%H:%M:%S')}")
            time.sleep(TESTE_INTERVALO_MINUTOS * 60)
            executar_rotina()
        else:
            agendado = proxima_execucao_prod()
            print(f"Modo Producao -> Proximo horario noturno agendado para: {agendado.strftime('%d/%m/%Y %H:%M:%S')}")
            segundos_espera = (agendado - datetime.now()).total_seconds()
            if segundos_espera > 0:
                time.sleep(segundos_espera)
            executar_rotina()


if __name__ == "__main__":
    main()