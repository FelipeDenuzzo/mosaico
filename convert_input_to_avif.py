import os
from pathlib import Path

from PIL import Image
import pillow_avif  # registra AVIF no Pillow
import pillow_heif  # registra HEIF/HEIC no Pillow

INPUT_DIR = Path("input")  # pasta raiz do projeto onde os uploads são salvos

IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".jpe", ".jfif", ".jif",
    ".png",
    ".heic", ".heif", ".hif",
    ".webp",
    ".avif",
}

AVIF_QUALITY = 80


def eh_imagem_suportada(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in IMAGE_EXTENSIONS


def converter_para_avif(caminho_origem: Path) -> Path:
    destino = caminho_origem.with_suffix(".avif")

    if destino.exists():
        print(f"Pulado (já existe AVIF): {destino.name}")
        return destino

    try:
        with Image.open(caminho_origem) as img:
            if img.mode not in ("RGB", "L"):
                img = img.convert("RGB")

            img.save(destino, "AVIF", quality=AVIF_QUALITY)
            print(f"Convertido: {caminho_origem.name} -> {destino.name}")
            return destino
    except Exception as e:
        print(f"Erro ao converter {caminho_origem}: {e}")
        return caminho_origem


def converter_todas_entradas_para_avif():
    if not INPUT_DIR.exists():
        print(f"Pasta de entrada não encontrada: {INPUT_DIR.resolve()}")
        return

    for entry in INPUT_DIR.iterdir():
        if eh_imagem_suportada(entry):
            converter_para_avif(entry)


if __name__ == "__main__":
    converter_todas_entradas_para_avif()
