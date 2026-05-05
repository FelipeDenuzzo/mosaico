import os
import time
import shutil
from pathlib import Path

from criar_mosaico import criar_mosaico  # ajuste para o módulo real

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "input"
PROCESSING_DIR = BASE_DIR / "processing"
OUTPUT_DIR = BASE_DIR / "output"
ARCHIVE_DIR = BASE_DIR / "archive"

POLL_INTERVAL_SECONDS = 2


def ensure_dirs():
    for d in [INPUT_DIR, PROCESSING_DIR, OUTPUT_DIR, ARCHIVE_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def copy_to_archive(src_path: Path) -> Path:
    """
    Copia a imagem original vinda do UX para archive/,
    sem alterar o arquivo em input/.
    """
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    dest_path = ARCHIVE_DIR / src_path.name

    if dest_path.exists():
        stem = src_path.stem
        suffix = src_path.suffix
        dest_path = ARCHIVE_DIR / f"{stem}_{int(time.time())}{suffix}"

    shutil.copy2(src_path, dest_path)
    print(f"[watcher] Copiado para archive: {dest_path}")
    return dest_path


def process_file(input_path: Path):
    print(f"[watcher] Novo arquivo em input: {input_path}")

    # 1) Copia o original do UX para archive (funil de tiles futuros)
    copy_to_archive(input_path)

    # 2) Move o original para processing (fluxo atual do mosaico)
    PROCESSING_DIR.mkdir(parents=True, exist_ok=True)
    processing_path = PROCESSING_DIR / input_path.name
    shutil.move(str(input_path), processing_path)
    print(f"[watcher] Movido para processing: {processing_path}")

    # 3) Gera o mosaico usando a imagem base em alta
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"mosaico_{processing_path.name}"

    try:
        criar_mosaico(str(processing_path), str(output_path))
        print(f"[watcher] Mosaico gerado em: {output_path}")
    except Exception as e:
        print(f"[watcher] ERRO ao gerar mosaico para {processing_path}: {e}")


def watch_input():
    ensure_dirs()
    print("[watcher] Monitorando input/")

    known_files = set()

    while True:
        current_files = {
            f for f in INPUT_DIR.iterdir()
            if f.is_file() and not f.name.startswith(".")
        }

        new_files = current_files - known_files

        for f in sorted(new_files):
            process_file(f)

        known_files = current_files
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    watch_input()