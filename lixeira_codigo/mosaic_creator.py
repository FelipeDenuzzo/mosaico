import json
import traceback
PASTA_ACERVO_TILES = "/Users/felipedenuzzo/VSCODE/Mosaico Programas/acervo"
TILES_BASE = PASTA_ACERVO_TILES


def process_jobs(jobs_path="../Site/jobs.json", tiles_folder=PASTA_ACERVO_TILES):
    """
    Processa todos os jobs com status 'recebido' ou 'processando'.
    Atualiza jobs.json com status, erro e output_url.
    """
    jobs_path = os.path.abspath(jobs_path)
    if not os.path.exists(jobs_path):
        print(f"Arquivo jobs.json não encontrado: {jobs_path}")
        return
    with open(jobs_path, "r", encoding="utf-8") as f:
        jobs = json.load(f)
    alterou = False
    for job_id, job in jobs.items():
        if job["status"] not in ("recebido", "processando"):
            continue
        print(f"Processando job: {job_id}")
        jobs[job_id]["status"] = "processando"
        jobs[job_id]["erro"] = None
        alterou = True
        try:
            input_path = job["input_path"]
            input_basename = os.path.basename(input_path)
            root_dir = os.path.dirname(os.path.dirname(jobs_path))
            output_dir = os.path.join(root_dir, "Output")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, input_basename)
            jobs[job_id]["output_path"] = output_path
            # tiles_folder pode ser passado, senão usa o acervo centralizado
            tfolder = tiles_folder or TILES_BASE
            # Parâmetros fixos do sistema
            build_mosaic(
                reference_path=input_path,
                tiles_folder=tfolder,
                tile_size_mm=FIXED_TILE_SIZE,
                max_uses=FIXED_MAX_USES,
                output_path=output_path,
                similarity=FIXED_SIMILARITY,
                quality=85,
            )
            jobs[job_id]["status"] = "pronto"
            jobs[job_id]["output_url"] = f"/output/{os.path.basename(output_path)}"
            jobs[job_id]["erro"] = None
            print(f"✅ Job {job_id} finalizado!")
        except Exception as e:
            jobs[job_id]["status"] = "erro"
            jobs[job_id]["erro"] = str(e) + "\n" + traceback.format_exc()
            print(f"❌ Erro no job {job_id}: {e}")
        alterou = True
    if alterou:
        with open(jobs_path, "w", encoding="utf-8") as f:
            json.dump(jobs, f, indent=2, ensure_ascii=False)
    else:
        print("Nenhum job pendente para processar.")
import argparse
import math
import os
from dataclasses import dataclass
from typing import Iterable, List, Tuple

from PIL import Image


FIXED_TILES = 146
FIXED_TILE_SIZE = 30
FIXED_OUTPUT_SIZE = FIXED_TILES * FIXED_TILE_SIZE
FIXED_SIMILARITY = 10
FIXED_MAX_USES = 2
MIN_REFERENCE_WIDTH = 1000


@dataclass
class TileInfo:
    path: str
    average_color: Tuple[int, int, int]
    image: Image.Image
    uses: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Cria um mosaico a partir de uma imagem base usando outras imagens como pixels."
        )
    )
    parser.add_argument(
        "--reference",
        required=False,
        help="Caminho da imagem base que será recriada.",
    )
    parser.add_argument(
        "--tiles",
        required=False,
        help="Pasta com imagens que serão usadas como pixels.",
    )
    parser.add_argument(
        "--pixel-size",
        type=int,
        choices=[25, 50],
        help="Tamanho do pixel em mm (25mm ou 50mm).",
    )
    parser.add_argument(
        "--max-uses",
        type=int,
        choices=[0, 2, 4],
        help="Limite de repetições por imagem (0, 2 ou 4).",
    )
    parser.add_argument(
        "--output",
        default="mosaico_final.jpg",
        help="Arquivo de saída do mosaico.",
    )
    parser.add_argument(
        "--quality",
        type=int,
        default=85,
        help="Qualidade JPEG (1-100). Padrão: 85.",
    )
    parser.add_argument(
        "--similarity",
        type=float,
        default=0.0,
        help=(
            "Nível de similaridade de cores (0-1). "
            "0 = qualquer cor, 1 = cores muito similares. Melhora definição."
        ),
    )
    return parser.parse_args()


def mm_to_pixels(mm: int, dpi: int = 240) -> int:
    """Converte milímetros para pixels com base no DPI."""
    inches = mm / 25.4
    return round(inches * dpi)


def pixels_to_cm(pixels: int, dpi: int = 240) -> float:
    """Converte pixels para centímetros com base no DPI."""
    inches = pixels / dpi
    return inches * 2.54


def calculate_final_size(
    reference_path: str,
    tile_size_mm: int,
    tiles_folder: str | None = None,
    dpi: int = 240,
) -> Tuple[int, int, float, float]:
    """
    Calcula o tamanho final em pixels e centímetros usando:
    - Grid determinado pelo tamanho do pixel escolhido (mm -> px) para análise
    - Tamanho nativo dos tiles para composição (sem redimensionar)
    Retorna: (pixels_width, pixels_height, cm_width, cm_height)
    """
    with Image.open(reference_path) as reference:
        ref_width = reference.width
        ref_height = reference.height

    tile_analysis_px = mm_to_pixels(tile_size_mm, dpi)

    # Quantidade de tiles que cabem na referência para análise
    tiles_x = max(1, ref_width // tile_analysis_px)
    tiles_y = max(1, ref_height // tile_analysis_px)

    # Tamanho nativo do tile (usaremos o primeiro como referência)
    sample_size = 100
    if tiles_folder:
        try:
            from PIL import Image as PILImage
            supported = (".png", ".jpg", ".jpeg")
            for name in os.listdir(tiles_folder):
                if name.lower().endswith(supported):
                    with PILImage.open(os.path.join(tiles_folder, name)) as sample:
                        sample_size = sample.width
                        break
        except Exception:
            pass

    # Tamanho final em pixels com base no tamanho nativo do tile
    final_width_px = tiles_x * sample_size
    final_height_px = tiles_y * sample_size

    # Converter para centímetros
    final_width_cm = pixels_to_cm(final_width_px, dpi)
    final_height_cm = pixels_to_cm(final_height_px, dpi)

    return final_width_px, final_height_px, final_width_cm, final_height_cm


def interactive_setup(reference_path: str, tiles_folder: str) -> Tuple[int, int, float, str]:
    """
    Interface interativa para o usuário escolher tamanho de pixel e repetições.
    Retorna: (pixel_size_mm, max_uses, similarity, output_path)
    """
    print("\n" + "=" * 60)
    print("CONFIGURAÇÃO DO MOSAICO")
    print("=" * 60)
    
    # Contar imagens na pasta
    supported = (".png", ".jpg", ".jpeg")
    num_tiles = len([
        name for name in os.listdir(tiles_folder)
        if name.lower().endswith(supported)
    ])
    
    print(f"\n📁 Imagens disponíveis: {num_tiles} arquivos")
    
    # Escolher tamanho de pixel (qualquer valor inteiro positivo)
    print("\n📏 Escolha o tamanho do pixel (em mm):")
    while True:
        try:
            pixel_size_mm = int(input("Digite o tamanho do pixel em mm (ex: 25): ").strip())
            if pixel_size_mm > 0:
                break
            print("❌ Valor inválido. Digite um número positivo.")
        except ValueError:
            print("❌ Valor inválido. Digite um número inteiro.")

    # Calcular e exibir tamanho final
    width_px, height_px, width_cm, height_cm = calculate_final_size(
        reference_path, pixel_size_mm, tiles_folder
    )

    print(f"\n📐 Tamanho final da imagem com pixel de {pixel_size_mm}mm:")
    print(f"   • Pixels: {width_px} x {height_px} px")
    print(f"   • Centímetros: {width_cm:.1f} x {height_cm:.1f} cm")
    print(f"   • Polegadas: {width_cm/2.54:.1f} x {height_cm/2.54:.1f} in")

    # Escolher limite de repetições
    print("\n🔁 Escolha o limite de repetições por imagem:")
    print("  1) 0 (cada imagem usa apenas 1 vez)")
    print("  2) 2 (cada imagem pode ser usada até 2 vezes)")
    print("  3) 4 (cada imagem pode ser usada até 4 vezes)")

    while True:
        uses_choice = input("\nOpção (1, 2 ou 3): ").strip()
        if uses_choice == "1":
            max_uses = 0
            break
        elif uses_choice == "2":
            max_uses = 2
            break
        elif uses_choice == "3":
            max_uses = 4
            break
        print("❌ Opção inválida. Digite 1, 2 ou 3.")

    repetition_text = (
        "sem repetições"
        if max_uses == 0
        else f"máximo {max_uses} repetições por imagem"
    )
    print(f"✅ Configuração: {repetition_text}")

    # Perguntar variação de cor (0-100)
    while True:
        try:
            similarity = float(input("\nVariação de cor (0-100, ex: 0 para só o mais próximo, 100 para qualquer tile): ").strip())
            if 0.0 <= similarity <= 100.0:
                break
            print("❌ Valor inválido. Digite um número entre 0 e 100.")
        except ValueError:
            print("❌ Valor inválido. Digite um número entre 0 e 100.")

    # Perguntar nome do arquivo de saída
    output_path = input("\nNome do arquivo de saída (ex: mosaico_final.jpg): ").strip()
    if not output_path:
        output_path = "mosaico_final.jpg"
    print("\n" + "=" * 60 + "\n")

    return pixel_size_mm, max_uses, similarity, output_path


def list_image_files(folder: str) -> List[str]:
    supported = (".png", ".jpg", ".jpeg")
    return [
        os.path.join(folder, name)
        for name in os.listdir(folder)
        if name.lower().endswith(supported)
    ]


def average_color(image: Image.Image) -> Tuple[int, int, int]:
    small = image.convert("RGB").resize((1, 1), Image.Resampling.LANCZOS)
    return small.getpixel((0, 0))


def load_tiles(folder: str) -> List[TileInfo]:
    tiles = []
    for path in list_image_files(folder):
        with Image.open(path) as image:
            # NÃO redimensionar - manter tamanho original
            rgb_image = image.convert("RGB")
            tiles.append(
                TileInfo(
                    path=path,
                    average_color=average_color(rgb_image),
                    image=rgb_image.copy(),
                )
            )
    return tiles


def distance(color_a: Tuple[int, int, int], color_b: Tuple[int, int, int]) -> float:
    return math.sqrt(
        sum((color_a[index] - color_b[index]) ** 2 for index in range(3))
    )


def select_tile(
    target_color: Tuple[int, int, int],
    tiles: Iterable[TileInfo],
    max_uses: int,
    last_color: Tuple[int, int, int] = None,
    similarity: float = 0.0,
) -> TileInfo:
    import random
    best_tile = None
    best_distance = float("inf")
    tiles_list = list(tiles)
    # Calcula distâncias
    dists = []
    for tile in tiles_list:
        if max_uses and tile.uses >= max_uses:
            continue
        diff = distance(target_color, tile.average_color)
        dists.append((tile, diff))
    if not dists:
        dists = [(tile, distance(target_color, tile.average_color)) for tile in tiles_list]
    if not dists:
        raise RuntimeError("Nenhuma imagem disponível para continuar o mosaico.")
    min_dist = min(d[1] for d in dists)
    max_dist = 442  # sqrt(3*255^2)
    lim = min_dist + (max_dist - min_dist) * (similarity / 100)
    candidatos = [t for t, d in dists if d <= lim]
    if candidatos:
        return random.choice(candidatos)
    return min(dists, key=lambda d: d[1])[0]


def build_mosaic(
    reference_path: str,
    tiles_folder: str,
    tile_size_mm: int,
    max_uses: int,
    output_path: str,
    similarity: float = 0.0,
    quality: int = 85,
    dpi: int = 240,
) -> None:
    if not tiles_folder:
        tiles_folder = TILES_BASE

    tiles = load_tiles(tiles_folder)
    if not tiles:
        raise RuntimeError("Nenhuma imagem encontrada na pasta de tiles.")

    # Regras fixas do sistema
    max_uses = FIXED_MAX_USES
    similarity = FIXED_SIMILARITY
    tiles_x = FIXED_TILES
    tiles_y = FIXED_TILES

    with Image.open(reference_path) as reference:
        reference = reference.convert("RGB")
        if reference.width < MIN_REFERENCE_WIDTH:
            raise ValueError("A imagem base deve ter no mínimo 1000 px de largura.")

        # Recorte central para 1:1 sem distorcer antes da análise de cor
        ref_w, ref_h = reference.size
        lado = min(ref_w, ref_h)
        left = (ref_w - lado) // 2
        top = (ref_h - lado) // 2
        reference = reference.crop((left, top, left + lado, top + lado))

        # Pré-processa para grid fixo e lê cor com getpixel (mais rápido que crop por célula)
        grid_ref = reference.resize((FIXED_TILES, FIXED_TILES), Image.Resampling.BOX).convert("RGB")

    final_width = FIXED_OUTPUT_SIZE
    final_height = FIXED_OUTPUT_SIZE
    
    print(f"\n📊 Criando mosaico de {final_width}×{final_height} pixels...")
    print(f"   Grid: {tiles_x} × {tiles_y} tiles")
    print(f"   Tamanho nativo de cada tile: {FIXED_TILE_SIZE}×{FIXED_TILE_SIZE} px")
    
    if final_width > 65500 or final_height > 65500:
        print(f"⚠️  Aviso: tamanho ultrapassa limite PIL (65500 px)")
    
    mosaic = Image.new("RGB", (final_width, final_height), "white")
    last_color = None

    try:
        from importlib import import_module

        tqdm = import_module("tqdm").tqdm
    except Exception:
        def tqdm(x, **kwargs):
            return x

    for row in tqdm(range(tiles_y), desc='Linhas'):
        for col in range(tiles_x):
            # Posição na imagem final (com tamanho real dos pixels)
            left_final = col * FIXED_TILE_SIZE
            top_final = row * FIXED_TILE_SIZE

            # Cor da célula diretamente do grid de análise
            cell_color = grid_ref.getpixel((col, row))

            # Encontrar melhor tile
            tile = select_tile(cell_color, tiles, max_uses, last_color, similarity)
            tile.uses += 1
            last_color = cell_color

            # Colocar tile já carregado em memória
            mosaic.paste(tile.image, (left_final, top_final))

    # Salvar como JPEG
    print(f"💾 Salvando mosaico em {output_path}...")
    mosaic.save(output_path, 'JPEG', quality=quality)
    print(f"✅ Mosaico criado com sucesso!")


def main() -> None:
    args = parse_args()

    # Se não forneceu imagem base, abrir janela para selecionar
    reference_path = args.reference
    if not reference_path:
        try:
            import tkinter as tk
            from tkinter import filedialog
            root = tk.Tk()
            root.withdraw()
            print("Selecione a imagem base...")
            reference_path = filedialog.askopenfilename(title="Selecione a imagem base", filetypes=[("Imagens", "*.jpg *.jpeg *.png")])
            if not reference_path:
                print("Nenhuma imagem selecionada. Encerrando.")
                return
        except Exception as e:
            print(f"Erro ao abrir janela de seleção: {e}")
            return

    # Se não forneceu pasta de tiles, usar o acervo centralizado
    tiles_folder = args.tiles
    if not tiles_folder:
        tiles_folder = TILES_BASE

    # Se não forneceu pixel_size ou max_uses, usar modo interativo
    if args.pixel_size is None or args.max_uses is None:
        pixel_size_mm, max_uses, similarity, output_path = interactive_setup(reference_path, tiles_folder)
    else:
        pixel_size_mm = args.pixel_size
        max_uses = args.max_uses
        similarity = args.similarity
        output_path = args.output
    build_mosaic(
        reference_path=reference_path,
        tiles_folder=tiles_folder,
        tile_size_mm=pixel_size_mm,
        max_uses=max_uses,
        output_path=output_path,
        similarity=similarity,
        quality=args.quality,
    )
    print(f"✅ Mosaico criado com sucesso: {output_path}")
    print(f"📊 Qualidade JPEG: {args.quality}")


if __name__ == "__main__":
    import sys

    if "--process-jobs" in sys.argv:
        # Permite passar pasta de tiles opcional: --tiles <pasta>
        tiles_folder = None
        if "--tiles" in sys.argv:
            idx = sys.argv.index("--tiles")
            if idx+1 < len(sys.argv):
                tiles_folder = sys.argv[idx+1]
        process_jobs(jobs_path="../Site/jobs.json", tiles_folder=tiles_folder)
    elif len(sys.argv) == 1:
        import tkinter as tk
        from interface import InterfaceMosaico

        janela = tk.Tk()
        InterfaceMosaico(janela)
        janela.mainloop()
    else:
        main()
