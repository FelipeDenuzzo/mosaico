"""
Módulo principal para criação de mosaicos de imagens.
Lê uma imagem base e a recria usando imagens de pixels.
"""
from typing import Callable, Dict, List, Optional, Tuple

import os
import sqlite3
import unicodedata
import time
import tempfile
import mmap
from collections import defaultdict
from datetime import datetime
from dataclasses import dataclass
from functools import lru_cache
from heapq import nsmallest
from threading import Lock
import math
from PIL import Image
import pillow_avif  # registra suporte AVIF no Pillow


FIXED_COLUMNS = 146
FIXED_TILE_SIZE = 30
FIXED_COLOR_VARIATION = 20
FIXED_MAX_REPETITIONS = 2
MIN_BASE_WIDTH = 1000
FIXED_OUTPUT_WIDTH = FIXED_COLUMNS * FIXED_TILE_SIZE
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_INDEX_DB_PATH = os.path.join(PROJECT_ROOT, "Index", "tiles_index.db")
INDEX_DB_PATH = os.getenv("MOSAICO_INDEX_DB_PATH", DEFAULT_INDEX_DB_PATH)
SQLITE_PREFILTER_LIMIT = 180
COLOR_BUCKET_SIZE = 16
TILE_IMAGE_CACHE_MAXSIZE = max(5000, int(os.getenv("MOSAICO_TILE_CACHE_MAXSIZE", "15000")))
STRIP_FLUSH_ROWS = max(1, int(os.getenv("MOSAICO_STRIP_ROWS", "20")))
ENABLE_STRIP_RENDER = os.getenv("MOSAICO_STRIP_RENDER", "1") == "1"
DEBUG_VERBOSE_CELLS = os.getenv("MOSAICO_DEBUG_VERBOSE_CELLS", "0") == "1"
ENABLE_TILE_PRELOAD = os.getenv("MOSAICO_TILE_PRELOAD", "1") == "1"
DEBUG_LOG_PATH = os.path.join(os.path.dirname(__file__), "debug_mosaico.log")
PRODUCTION_LOG_PATH = os.path.join(os.path.dirname(__file__), "producao_mosaico.log")

# --------------------------------------------------
# Priorização por recência (ajuste opcional)
# --------------------------------------------------
RECENCIA_BONUS_MAX = 8          # desconto máximo na distância² de cor
RECENCIA_TOP_N = 20             # nº de candidatos onde o bônus atua
RECENCIA_JANELA_DIAS = 30       # últimos N dias contam como “recentes”


_CATALOGO_CACHE_LOCK = Lock()
_CATALOGO_CACHE_DB_FINGERPRINT: Optional[Tuple[int, int]] = None
# path, (r,g,b), created_at
_CATALOGO_CACHE_BY_CATEGORY: Dict[str, List[Tuple[str, Tuple[int, int, int], str]]] = {}
_TILE_PRELOAD_LOCK = Lock()
_TILE_PRELOAD_FINGERPRINT: Optional[Tuple[Optional[Tuple[int, int]], int]] = None
_TILE_PRELOAD_STORE: Dict[str, Image.Image] = {}


def invalidar_cache_catalogo() -> None:
    """
    Força recarga completa do acervo na próxima chamada a
    _carregar_catalogo_descritores_cacheado() e ao preload de tiles.
    Deve ser chamada após inserção de novos tiles no banco.
    """
    global _CATALOGO_CACHE_DB_FINGERPRINT, _TILE_PRELOAD_FINGERPRINT
    with _CATALOGO_CACHE_LOCK:
        _CATALOGO_CACHE_DB_FINGERPRINT = None
        _CATALOGO_CACHE_BY_CATEGORY.clear()
    with _TILE_PRELOAD_LOCK:
        _TILE_PRELOAD_FINGERPRINT = None
        _TILE_PRELOAD_STORE.clear()


def debug_log(msg: str) -> None:
    with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(str(msg) + "\n")


def production_log(msg: str) -> None:
    with open(PRODUCTION_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(str(msg) + "\n")


_TERMINAL_LOG_LOCK = Lock()
_TERMINAL_PROGRESS_ACTIVE = False


def terminal_log(msg: str, level: str = "INFO", arquivo: Optional[str] = None) -> None:
    """Log simples com timestamp para acompanhar execução no terminal."""
    global _TERMINAL_PROGRESS_ACTIVE
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefixo_arquivo = f"[{arquivo}] " if arquivo else ""
    linha = f"{ts} - {level} - {prefixo_arquivo}{msg}"
    with _TERMINAL_LOG_LOCK:
        if _TERMINAL_PROGRESS_ACTIVE:
            print("", flush=True)
            _TERMINAL_PROGRESS_ACTIVE = False
        print(linha, flush=True)


class TerminalProgress:
    """Barra de progresso compacta atualizada na mesma linha do terminal."""

    def __init__(self, etapa: str, total: int, arquivo: Optional[str] = None, width: int = 28):
        self.etapa = etapa
        self.total = max(1, int(total))
        self.arquivo = arquivo
        self.width = max(10, int(width))
        self.current = 0
        self._started = False

    def _render_line(self, valor: int) -> str:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        prefixo_arquivo = f"[{self.arquivo}] " if self.arquivo else ""
        progresso = max(0, min(valor, self.total))
        ratio = progresso / self.total
        preenchido = int(self.width * ratio)
        barra = "#" * preenchido + "-" * (self.width - preenchido)
        perc = int(ratio * 100)
        return (
            f"\r{ts} - INFO - {prefixo_arquivo}{self.etapa}: "
            f"[{barra}] {perc:3d}% ({progresso}/{self.total})"
        )

    def start(self) -> None:
        global _TERMINAL_PROGRESS_ACTIVE
        with _TERMINAL_LOG_LOCK:
            self._started = True
            _TERMINAL_PROGRESS_ACTIVE = True
            print(self._render_line(0), end="", flush=True)

    def update(self, valor: int) -> None:
        global _TERMINAL_PROGRESS_ACTIVE
        if not self._started:
            self.start()
        novo = max(0, min(int(valor), self.total))
        if novo == self.current:
            return
        self.current = novo
        with _TERMINAL_LOG_LOCK:
            _TERMINAL_PROGRESS_ACTIVE = True
            print(self._render_line(self.current), end="", flush=True)

    def close(self) -> None:
        global _TERMINAL_PROGRESS_ACTIVE
        if not self._started:
            return
        if self.current < self.total:
            self.update(self.total)
        with _TERMINAL_LOG_LOCK:
            print("", flush=True)
            _TERMINAL_PROGRESS_ACTIVE = False
        self._started = False


def _log_query_plan(conn: sqlite3.Connection, query_name: str, sql: str, params: tuple) -> list:
    plan = conn.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()
    debug_log(f"QUERY_NAME={query_name}")
    debug_log(f"QUERY_SQL={sql}")
    debug_log(f"QUERY_PARAMS={params}")
    debug_log(f"QUERY_PLAN={plan}")
    return plan


@dataclass
class TileInfo:
    """Informações sobre uma imagem de pixel."""
    path: str
    average_color: Tuple[int, int, int]
    uses: int = 0
    created_at: str = ""   # ISO 8601 ou vazio se não houver


def obter_cor_media(imagem: Image.Image) -> Tuple[int, int, int]:
    """Calcula a cor média de uma imagem."""
    pequena = imagem.convert("RGB").resize((1, 1), Image.Resampling.LANCZOS)
    return pequena.getpixel((0, 0))


def calcular_distancia_cor(cor_a: Tuple[int, int, int], cor_b: Tuple[int, int, int]) -> float:
    """Calcula a distância Euclidiana entre duas cores RGB."""
    return math.sqrt(sum((cor_a[i] - cor_b[i]) ** 2 for i in range(3)))


def _distancia_cor_quadrada(cor_a: Tuple[int, int, int], cor_b: Tuple[int, int, int]) -> int:
    dr = cor_a[0] - cor_b[0]
    dg = cor_a[1] - cor_b[1]
    db = cor_a[2] - cor_b[2]
    return (dr * dr) + (dg * dg) + (db * db)


def carregar_pixels(conn: sqlite3.Connection) -> List[TileInfo]:
    """
    Carrega metadados de todos os tiles do índice SQLite.
    Não varre pastas manualmente; usa apenas o banco de dados.
    """
    sql = """SELECT path, r, g, b, created_at FROM tiles"""
    rows = conn.execute(sql).fetchall()
    return [
        TileInfo(path=row[0], average_color=(row[1], row[2], row[3]), created_at=row[4] or "")
        for row in rows
    ]


def _normalizar_texto(valor: str) -> str:
    base = unicodedata.normalize("NFD", valor or "")
    sem_acentos = "".join(ch for ch in base if unicodedata.category(ch) != "Mn")
    return sem_acentos.lower()


def _validar_categoria_index(categoria: str) -> bool:
    """
    Mantida por compatibilidade com outros módulos legados.
    """
    categorias_validas = {"Informacao", "Medicamentos", "Pornografia", "Geral"}
    return categoria in categorias_validas


def _margem_cor(variacao_cor: int) -> int:
    # 0 -> faixa pequena, 100 -> faixa ampla
    return max(8, min(64, int(8 + (variacao_cor / 100.0) * 56)))


def _bucket_rgb(cor: Tuple[int, int, int], bucket_size: int = COLOR_BUCKET_SIZE) -> Tuple[int, int, int]:
    return (
        cor[0] // bucket_size,
        cor[1] // bucket_size,
        cor[2] // bucket_size,
    )


def _carregar_catalogo_categoria_index(
    conn: sqlite3.Connection,
    categoria: str,
) -> List[TileInfo]:
    t_sql = time.perf_counter()
    sql = """
        SELECT path, r, g, b, created_at
        FROM tiles
        WHERE categoria = ?
    """
    params = (categoria,)
    _log_query_plan(conn, "catalogo_categoria", sql, params)
    rows = conn.execute(sql, params).fetchall()
    debug_log(f"SQLITE_FULL_LOAD_TIME_S={time.perf_counter() - t_sql:.6f}")
    debug_log(f"CATALOGO_SQLITE={len(rows)}")
    return [
        TileInfo(path=row[0], average_color=(row[1], row[2], row[3]), created_at=row[4] or "")
        for row in rows
    ]


def _construir_buckets_por_cor(
    tiles: List[TileInfo],
    bucket_size: int = COLOR_BUCKET_SIZE,
) -> Dict[Tuple[int, int, int], List[TileInfo]]:
    buckets: Dict[Tuple[int, int, int], List[TileInfo]] = defaultdict(list)
    for tile in tiles:
        buckets[_bucket_rgb(tile.average_color, bucket_size)].append(tile)
    return dict(buckets)


def _db_fingerprint(index_db_path: str) -> Tuple[int, int]:
    stat = os.stat(index_db_path)
    return (stat.st_mtime_ns, stat.st_size)


def _carregar_catalogo_descritores_cacheado() -> Tuple[List[Tuple[str, Tuple[int, int, int], str]], bool]:
    if not os.path.exists(INDEX_DB_PATH):
        raise ValueError(f"Banco de index não encontrado: {INDEX_DB_PATH}")

    db_fp = _db_fingerprint(INDEX_DB_PATH)
    with _CATALOGO_CACHE_LOCK:
        global _CATALOGO_CACHE_DB_FINGERPRINT
        if _CATALOGO_CACHE_DB_FINGERPRINT != db_fp:
            _CATALOGO_CACHE_BY_CATEGORY.clear()
            _CATALOGO_CACHE_DB_FINGERPRINT = db_fp

        existente = _CATALOGO_CACHE_BY_CATEGORY.get("acervo")
        if existente is not None:
            return existente, True

    with sqlite3.connect(INDEX_DB_PATH) as conn:
        rows = conn.execute("SELECT path, r, g, b, created_at FROM tiles").fetchall()
        descritores = [
            (row[0], (int(row[1]), int(row[2]), int(row[3])), row[4] or "")
            for row in rows
        ]

    with _CATALOGO_CACHE_LOCK:
        _CATALOGO_CACHE_BY_CATEGORY["acervo"] = descritores
    return descritores, False


def _preaquecer_cache_tiles_renderizados(
    descritores_catalogo: List[Tuple[str, Tuple[int, int, int], str]],
    tamanho_pixel: int,
) -> Tuple[int, float, bool]:
    """
    Precarrega tiles 30x30 na RAM para reduzir I/O no loop principal.
    Retorna (qtd_carregada, tempo_s, cache_hit_global).
    """
    db_fp = _db_fingerprint(INDEX_DB_PATH)
    fingerprint = (db_fp, int(tamanho_pixel))

    with _TILE_PRELOAD_LOCK:
        global _TILE_PRELOAD_FINGERPRINT
        if _TILE_PRELOAD_FINGERPRINT == fingerprint and _TILE_PRELOAD_STORE:
            return len(_TILE_PRELOAD_STORE), 0.0, True

    inicio = time.perf_counter()
    novo_store: Dict[str, Image.Image] = {}
    for path, _rgb, _created_at in descritores_catalogo:
        try:
            with Image.open(path) as img:
                tile = img.convert("RGB")
                if tile.size != (tamanho_pixel, tamanho_pixel):
                    tile = tile.resize((tamanho_pixel, tamanho_pixel), Image.Resampling.LANCZOS)
                novo_store[path] = tile.copy()
        except Exception:
            continue

    elapsed = time.perf_counter() - inicio
    with _TILE_PRELOAD_LOCK:
        _TILE_PRELOAD_STORE.clear()
        _TILE_PRELOAD_STORE.update(novo_store)
        _TILE_PRELOAD_FINGERPRINT = fingerprint

    return len(novo_store), elapsed, False


def _obter_tile_cacheado(path: str, tamanho_pixel: int) -> Image.Image:
    with _TILE_PRELOAD_LOCK:
        tile = _TILE_PRELOAD_STORE.get(path)
    if tile is not None:
        return tile
    return _carregar_tile_renderizado(path, tamanho_pixel)


def _buscar_candidatos_memoria(
    cor_alvo: Tuple[int, int, int],
    variacao_cor: int,
    buckets: Dict[Tuple[int, int, int], List[TileInfo]],
    todos_tiles: List[TileInfo],
    limite: int = SQLITE_PREFILTER_LIMIT,
) -> List[TileInfo]:
    t_mem = time.perf_counter()
    margem = _margem_cor(variacao_cor)
    r, g, b = cor_alvo
    r_min, r_max = max(0, r - margem), min(255, r + margem)
    g_min, g_max = max(0, g - margem), min(255, g + margem)
    b_min, b_max = max(0, b - margem), min(255, b + margem)

    bucket_alvo = _bucket_rgb(cor_alvo)
    alcance = max(1, math.ceil(margem / COLOR_BUCKET_SIZE))

    candidatos_bucket: List[TileInfo] = []
    for dr in range(-alcance, alcance + 1):
        for dg in range(-alcance, alcance + 1):
            for db in range(-alcance, alcance + 1):
                chave = (bucket_alvo[0] + dr, bucket_alvo[1] + dg, bucket_alvo[2] + db)
                candidatos_bucket.extend(buckets.get(chave, []))

    if not candidatos_bucket:
        candidatos_bucket = todos_tiles

    filtrados = [
        tile
        for tile in candidatos_bucket
        if r_min <= tile.average_color[0] <= r_max
        and g_min <= tile.average_color[1] <= g_max
        and b_min <= tile.average_color[2] <= b_max
    ]

    base_busca = filtrados if filtrados else candidatos_bucket
    if not base_busca:
        return []

    candidatos = nsmallest(
        limite,
        base_busca,
        key=lambda tile: (
            abs(tile.average_color[0] - r)
            + abs(tile.average_color[1] - g)
            + abs(tile.average_color[2] - b)
        ),
    )

    if DEBUG_VERBOSE_CELLS:
        debug_log(f"MEM_PREFILTER_TIME_S={time.perf_counter() - t_mem:.6f}")
        debug_log(f"CANDIDATOS_BUCKET={len(candidatos_bucket)}")
        debug_log(f"CANDIDATOS_FILTRADOS={len(filtrados)}")
        debug_log(f"CANDIDATOS_MEM={len(candidatos)}")
    return candidatos


@lru_cache(maxsize=TILE_IMAGE_CACHE_MAXSIZE)
def _carregar_tile_renderizado(path: str, tamanho_pixel: int) -> Image.Image:
    """Carrega tile físico sob demanda com cache LRU e tamanho final padronizado."""
    with Image.open(path) as img:
        tile = img.convert("RGB")
        if tile.size != (tamanho_pixel, tamanho_pixel):
            tile = tile.resize((tamanho_pixel, tamanho_pixel), Image.Resampling.LANCZOS)
        return tile.copy()


def _é_vizinho_válido(
    linha_atual: int,
    coluna_atual: int,
    tile_path: str,
    ultimas_posicoes: Dict[str, Tuple[int, int]],
) -> bool:
    """
    Verifica se um tile pode ser usado na posição atual respeitando 8-vizinhos.
    """
    if tile_path not in ultimas_posicoes:
        return True

    ultima_linha, ultima_coluna = ultimas_posicoes[tile_path]
    dist_linha = abs(linha_atual - ultima_linha)
    dist_coluna = abs(coluna_atual - ultima_coluna)

    if dist_linha > 1 or dist_coluna > 1:
        return True

    return False


def _bonus_recencia(created_at_str: str) -> float:
    """
    Retorna bônus de desconto na distância de cor (0 a RECENCIA_BONUS_MAX).
    Tiles inseridos nos últimos RECENCIA_JANELA_DIAS dias recebem bônus proporcional.
    """
    if not created_at_str:
        return 0.0
    try:
        criado = datetime.fromisoformat(created_at_str)
        idade_dias = (datetime.now() - criado).days
        if idade_dias >= RECENCIA_JANELA_DIAS:
            return 0.0
        fator = 1.0 - (idade_dias / RECENCIA_JANELA_DIAS)
        return RECENCIA_BONUS_MAX * fator
    except Exception:
        return 0.0


def selecionar_pixel(
    cor_alvo: Tuple[int, int, int],
    pixels: List[TileInfo],
    max_repeticoes: int,
    variacao_cor: int,
    linha_atual: int,
    coluna_atual: int,
    cor_anterior: Tuple[int, int, int] = None,
    ultimas_posicoes: Optional[Dict[str, Tuple[int, int]]] = None,
) -> TileInfo:
    """
    Seleciona o melhor tile para uma célula.
    Respeita restrições de 8-vizinhos e max_repeticoes.
    Entre os RECENCIA_TOP_N melhores candidatos por cor, privilegia tiles mais recentes.
    """
    ultimas_posicoes = ultimas_posicoes or {}

    def _score_recencia(pixel: TileInfo) -> float:
        dist = _distancia_cor_quadrada(cor_alvo, pixel.average_color)
        return dist - _bonus_recencia(pixel.created_at)

    # Caso 1: respeita max_repeticoes e vizinhança
    candidatos_ambas: List[TileInfo] = [
        p for p in pixels
        if (max_repeticoes <= 0 or p.uses < max_repeticoes)
        and _é_vizinho_válido(linha_atual, coluna_atual, p.path, ultimas_posicoes)
    ]
    if candidatos_ambas:
        top_n = nsmallest(
            RECENCIA_TOP_N,
            candidatos_ambas,
            key=lambda p: _distancia_cor_quadrada(cor_alvo, p.average_color),
        )
        return min(top_n, key=_score_recencia)

    # Caso 2: ignora vizinhança, respeita max_repeticoes
    candidatos_max_uses: List[TileInfo] = [
        p for p in pixels
        if max_repeticoes <= 0 or p.uses < max_repeticoes
    ]
    if candidatos_max_uses:
        if DEBUG_VERBOSE_CELLS:
            debug_log(f"FALLBACK_8VIZ=True CELULA={linha_atual},{coluna_atual}")
        top_n = nsmallest(
            RECENCIA_TOP_N,
            candidatos_max_uses,
            key=lambda p: _distancia_cor_quadrada(cor_alvo, p.average_color),
        )
        return min(top_n, key=_score_recencia)

    # Caso 3: ignora max_repeticoes (fallback total)
    if not pixels:
        raise RuntimeError("Nenhum pixel disponível!")

    min_uso = min(p.uses for p in pixels)
    menos_usados = [p for p in pixels if p.uses == min_uso]
    if DEBUG_VERBOSE_CELLS:
        debug_log(f"FALLBACK_MAX_USES=True CELULA={linha_atual},{coluna_atual}")
    top_n = nsmallest(
        RECENCIA_TOP_N,
        menos_usados,
        key=lambda p: _distancia_cor_quadrada(cor_alvo, p.average_color),
    )
    return min(top_n, key=_score_recencia)


def _iterar_faixas(total_linhas: int, linhas_por_faixa: int) -> List[Tuple[int, int]]:
    faixas: List[Tuple[int, int]] = []
    inicio = 0
    while inicio < total_linhas:
        fim = min(total_linhas, inicio + linhas_por_faixa)
        faixas.append((inicio, fim))
        inicio = fim
    return faixas


def _renderizar_faixa(
    linha_inicio: int,
    linha_fim: int,
    colunas: int,
    linhas_totais: int,
    tamanho_final_pixel: int,
    img_redimensionada: Image.Image,
    variacao_cor: int,
    max_repeticoes: int,
    buckets_por_cor: Dict[Tuple[int, int, int], List[TileInfo]],
    tiles_catalogo: List[TileInfo],
    ultimas_posicoes: Dict[str, Tuple[int, int]],
    cor_anterior: Optional[Tuple[int, int, int]],
    callback_progresso: Optional[Callable[[int, int], None]],
    cache_candidatos_por_cor: Optional[Dict[Tuple[int, int, int], List[TileInfo]]] = None,
    metricas_tempo: Optional[Dict[str, float]] = None,
) -> Tuple[Image.Image, Optional[Tuple[int, int, int]]]:
    faixa_altura_px = (linha_fim - linha_inicio) * tamanho_final_pixel
    faixa_largura_px = colunas * tamanho_final_pixel
    faixa_img = Image.new("RGB", (faixa_largura_px, faixa_altura_px), "white")

    total_celulas = colunas * linhas_totais

    for linha in range(linha_inicio, linha_fim):
        for coluna in range(colunas):
            celula_atual = linha * colunas + coluna + 1
            if callback_progresso:
                callback_progresso(celula_atual, total_celulas)

            cor_celula = img_redimensionada.getpixel((coluna, linha))
            if DEBUG_VERBOSE_CELLS:
                debug_log(f"CELULA={linha},{coluna}")
                debug_log(f"COR_ALVO={cor_celula}")

            t_sel_ini = time.perf_counter()

            pixels_candidatos = None
            if cache_candidatos_por_cor is not None:
                pixels_candidatos = cache_candidatos_por_cor.get(cor_celula)

            if pixels_candidatos is None:
                pixels_candidatos = _buscar_candidatos_memoria(
                    cor_celula,
                    variacao_cor,
                    buckets_por_cor,
                    tiles_catalogo,
                )
                if cache_candidatos_por_cor is not None:
                    cache_candidatos_por_cor[cor_celula] = pixels_candidatos

            if DEBUG_VERBOSE_CELLS:
                debug_log(f"CANDIDATOS_VALIDOS_MEM={len(pixels_candidatos)}")

            if not pixels_candidatos:
                raise ValueError("Nenhum candidato válido para montar o mosaico.")

            pixel_sel = selecionar_pixel(
                cor_celula,
                pixels_candidatos,
                max_repeticoes,
                variacao_cor,
                linha,
                coluna,
                cor_anterior,
                ultimas_posicoes,
            )
            if metricas_tempo is not None:
                metricas_tempo["selecao_tiles_s"] = metricas_tempo.get("selecao_tiles_s", 0.0) + (
                    time.perf_counter() - t_sel_ini
                )

            if DEBUG_VERBOSE_CELLS:
                debug_log(f"TILE_FINAL={pixel_sel.path}")

            pixel_sel.uses += 1
            ultimas_posicoes[pixel_sel.path] = (linha, coluna)
            cor_anterior = cor_celula

            x_final = coluna * tamanho_final_pixel
            y_faixa = (linha - linha_inicio) * tamanho_final_pixel

            t_comp_ini = time.perf_counter()
            tile_render = _obter_tile_cacheado(pixel_sel.path, tamanho_final_pixel)
            faixa_img.paste(tile_render, (x_final, y_faixa))
            if metricas_tempo is not None:
                metricas_tempo["composicao_final_s"] = metricas_tempo.get("composicao_final_s", 0.0) + (
                    time.perf_counter() - t_comp_ini
                )

    return faixa_img, cor_anterior


def _mesclar_faixas_para_saida(
    faixas_arquivos: List[Tuple[int, str]],
    largura_final: int,
    altura_final: int,
    tamanho_final_pixel: int,
    caminho_saida: str,
    qualidade: int,
) -> None:
    bytes_por_linha = largura_final * 3
    total_bytes = bytes_por_linha * altura_final

    with tempfile.NamedTemporaryFile(prefix="mosaic_frame_", suffix=".rgb", delete=False) as tmp_rgb:
        caminho_rgb = tmp_rgb.name
        tmp_rgb.truncate(total_bytes)

    try:
        with open(caminho_rgb, "r+b") as rgb_file:
            buffer_rgb = mmap.mmap(rgb_file.fileno(), total_bytes, access=mmap.ACCESS_WRITE)
            try:
                for linha_inicio, caminho_faixa in faixas_arquivos:
                    with Image.open(caminho_faixa) as faixa_img:
                        faixa_rgb = faixa_img.convert("RGB")
                        if faixa_rgb.width != largura_final:
                            raise ValueError(
                                f"Largura da faixa inválida: esperado {largura_final}, obtido {faixa_rgb.width}."
                            )

                        faixa_bytes = faixa_rgb.tobytes()
                        faixa_altura = faixa_rgb.height
                        tamanho_esperado = faixa_altura * bytes_por_linha
                        if len(faixa_bytes) != tamanho_esperado:
                            raise ValueError("Tamanho de bytes da faixa incompatível com o layout RGB esperado.")

                        offset = linha_inicio * tamanho_final_pixel * bytes_por_linha
                        buffer_rgb[offset:offset + tamanho_esperado] = faixa_bytes

                buffer_rgb.flush()

                imagem_final = Image.frombuffer(
                    "RGB",
                    (largura_final, altura_final),
                    buffer_rgb,
                    "raw",
                    "RGB",
                    0,
                    1,
                )
                try:
                    # [ALTERADO 2026-05] Salva em AVIF se o caminho de saída terminar com .avif
                    if str(caminho_saida).lower().endswith('.avif'):
                        imagem_final.save(caminho_saida, "AVIF", quality=80)
                    else:
                        imagem_final.save(caminho_saida, "JPEG", quality=qualidade)
                finally:
                    imagem_final.close()
            finally:
                buffer_rgb.close()
    finally:
        try:
            os.remove(caminho_rgb)
        except FileNotFoundError:
            pass


class MosaicMmapWriter:
    """
    Escreve faixas RGB diretamente no frame final mapeado em disco.
    """

    def __init__(self, largurafinal: int, alturafinal: int):
        self.largurafinal = largurafinal
        self.alturafinal = alturafinal
        self.bytesporlinha = largurafinal * 3
        self.totalbytes = self.bytesporlinha * alturafinal

        self.tmpfile = tempfile.NamedTemporaryFile(
            prefix="mosaicframe", suffix=".rgb", delete=False
        )
        self.tmppath = self.tmpfile.name
        self.tmpfile.truncate(self.totalbytes)
        self.tmpfile.close()

        self.rgbfile = open(self.tmppath, "r+b")
        self.buffer = mmap.mmap(self.rgbfile.fileno(), self.totalbytes, access=mmap.ACCESS_WRITE)

    def writestrip(self, linhainicio: int, faixaimg: Image.Image) -> None:
        faixargb = faixaimg if faixaimg.mode == "RGB" else faixaimg.convert("RGB")
        try:
            if faixargb.width != self.largurafinal:
                raise ValueError(
                    f"Largura da faixa inválida: esperado {self.largurafinal}, obtido {faixargb.width}."
                )

            faixabytes = faixargb.tobytes()
            faixaaltura = faixargb.height
            tamanhoesperado = faixaaltura * self.bytesporlinha

            if len(faixabytes) != tamanhoesperado:
                raise ValueError(
                    "Tamanho de bytes da faixa incompatível com o layout RGB esperado."
                )

            offset = linhainicio * self.bytesporlinha
            self.buffer[offset : offset + tamanhoesperado] = faixabytes
        finally:
            if faixargb is not faixaimg:
                faixargb.close()

    def save_image_avif(self, caminhosaida: str, qualidade: int = 30) -> None:
        """
        Salva o frame final em AVIF com a qualidade indicada.
        """
        self.buffer.flush()
        imagemfinal = Image.frombuffer(
            "RGB",
            (self.largurafinal, self.alturafinal),
            self.buffer,
            "raw",
            "RGB",
            0,
            1,
        )
        try:
            # formato AVIF via pillow-avif-plugin
            imagemfinal.save(caminhosaida, "AVIF", quality=qualidade)
        finally:
            imagemfinal.close()

    def close(self) -> None:
        try:
            self.buffer.close()
        finally:
            self.rgbfile.close()
        try:
            os.remove(self.tmppath)
        except FileNotFoundError:
            pass


class _MosaicMmapWriter(MosaicMmapWriter):
    """Compatibilidade com a API legada usada em criar_mosaico."""

    def write_strip(self, linha_inicio: int, faixa_img: Image.Image) -> None:
        self.writestrip(linha_inicio, faixa_img)

    def save_jpeg(self, caminho_saida: str, qualidade: int = 85) -> None:
        self.buffer.flush()
        imagem_final = Image.frombuffer(
            "RGB",
            (self.largurafinal, self.alturafinal),
            self.buffer,
            "raw",
            "RGB",
            0,
            1,
        )
        try:
            imagem_final.save(caminho_saida, "JPEG", quality=qualidade)
        finally:
            imagem_final.close()


def criar_mosaico(
    caminho_base: str,
    tamanho_pixel: int,
    redimensionar: bool,
    max_repeticoes: int,
    variacao_cor: int,
    caminho_saida: str,
    qualidade: int = 85,
    callback_progresso=None,
    usar_bandas: bool = ENABLE_STRIP_RENDER,
    linhas_por_banda: int = STRIP_FLUSH_ROWS,
) -> Tuple[int, int]:
    """
    Cria um mosaico a partir de uma imagem base.

    Args:
        caminho_base: Caminho da imagem base
        tamanho_pixel: Tamanho dos pixels em pixels (mantido por compatibilidade)
        redimensionar: Se True, redimensiona pixels para tamanho_pixel
        max_repeticoes: Máximo de repetições (0, 2, 4)
        variacao_cor: Variação de cor (0-100)
        caminho_saida: Onde salvar o arquivo final
        qualidade: Qualidade JPEG (1-100)
        callback_progresso: Função para reportar progresso

    Returns:
        Tupla (largura_final, altura_final) do mosaico
    """
    inicio_execucao = time.perf_counter()
    instante_execucao = datetime.now().isoformat(timespec="seconds")

    debug_log("=== NOVA EXECUCAO MOSAICO ===")
    debug_log(f"INDEX_DB_PATH={INDEX_DB_PATH}")
    debug_log(f"VARIACAO_COR={FIXED_COLOR_VARIATION}")

    production_log("=== PRODUCAO MOSAICO ===")
    production_log(f"DATA_HORA={instante_execucao}")
    production_log(f"FIXED_COLUMNS={FIXED_COLUMNS}")
    production_log(f"FIXED_TILE_SIZE={FIXED_TILE_SIZE}")
    production_log(f"FIXED_COLOR_VARIATION={FIXED_COLOR_VARIATION}")
    production_log(f"FIXED_MAX_REPETITIONS={FIXED_MAX_REPETITIONS}")
    production_log(f"MIN_BASE_WIDTH={MIN_BASE_WIDTH}")
    production_log(f"FIXED_OUTPUT_WIDTH={FIXED_OUTPUT_WIDTH}")
    production_log(f"INDEX_DB_PATH={INDEX_DB_PATH}")
    production_log(f"SQLITE_PREFILTER_LIMIT={SQLITE_PREFILTER_LIMIT}")
    production_log(f"MODO_BANDAS={usar_bandas}")
    production_log(f"LINHAS_POR_BANDA={linhas_por_banda}")
    production_log(f"RECENCIA_BONUS_MAX={RECENCIA_BONUS_MAX}")
    production_log(f"RECENCIA_TOP_N={RECENCIA_TOP_N}")
    production_log(f"RECENCIA_JANELA_DIAS={RECENCIA_JANELA_DIAS}")

    nome_arquivo_base = os.path.basename(caminho_base)
    terminal_log("Arquivo detectado para processamento.", arquivo=nome_arquivo_base)
    terminal_log("Inicio do processamento do mosaico.", arquivo=nome_arquivo_base)

    variacao_cor_efetiva = max(0, min(100, int(variacao_cor)))
    max_repeticoes_efetivo = max(0, int(max_repeticoes))

    production_log(f"VARIACAO_COR_PARAM={variacao_cor}")
    production_log(f"MAX_REPETICOES_PARAM={max_repeticoes}")
    production_log(f"VARIACAO_COR_EFETIVA={variacao_cor_efetiva}")
    production_log(f"MAX_REPETICOES_EFETIVO={max_repeticoes_efetivo}")

    t_leitura_base = time.perf_counter()
    with Image.open(caminho_base) as img_base:
        img_base = img_base.convert("RGB")
        largura_base = img_base.width
        altura_base = img_base.height

        ref_w, ref_h = img_base.size
        lado = min(ref_w, ref_h)
        left = (ref_w - lado) // 2
        top = (ref_h - lado) // 2
        img_base = img_base.crop((left, top, left + lado, top + lado))
        tempo_leitura_base = time.perf_counter() - t_leitura_base

        t_calculo_grid = time.perf_counter()
        img_redimensionada = img_base.resize((FIXED_COLUMNS, FIXED_COLUMNS), Image.Resampling.BOX)
        tempo_calculo_grid = time.perf_counter() - t_calculo_grid

    terminal_log(f"Etapa leitura_imagem_base: {tempo_leitura_base:.2f}s.", arquivo=nome_arquivo_base)
    terminal_log(f"Etapa calculo_grid: {tempo_calculo_grid:.2f}s.", arquivo=nome_arquivo_base)

    if largura_base < MIN_BASE_WIDTH:
        terminal_log(
            f"Erro final no processamento: imagem base com largura {largura_base}px (minimo {MIN_BASE_WIDTH}px).",
            level="ERROR",
            arquivo=nome_arquivo_base,
        )
        raise ValueError("A imagem base deve ter no mínimo 1000 px de largura.")

    colunas = FIXED_COLUMNS
    linhas = FIXED_COLUMNS
    tamanho_final_pixel = FIXED_TILE_SIZE

    ultimas_posicoes: Dict[str, Tuple[int, int]] = {}
    cache_candidatos_por_cor: Dict[Tuple[int, int, int], List[TileInfo]] = {}

    terminal_log("Carregamento do acervo iniciado.", arquivo=nome_arquivo_base)
    t_carregamento_acervo = time.perf_counter()
    progresso_indexacao = TerminalProgress("Indexacao de tiles", 3, arquivo=nome_arquivo_base)
    progresso_indexacao.start()

    progresso_indexacao.update(1)
    try:
        descritores_catalogo, cache_hit = _carregar_catalogo_descritores_cacheado()
    except Exception as exc:
        progresso_indexacao.close()
        terminal_log(
            f"Erro final no processamento: falha ao carregar acervo ({exc}).",
            level="ERROR",
            arquivo=nome_arquivo_base,
        )
        raise

    total_tiles = len(descritores_catalogo)
    if total_tiles == 0:
        progresso_indexacao.close()
        terminal_log("Erro final no processamento: acervo sem tiles no indice.", level="ERROR", arquivo=nome_arquivo_base)
        raise ValueError("Acervo sem tiles no index SQLite.")

    debug_log(f"USANDO_INDEX=True TOTAL_TILES={total_tiles}")
    terminal_log(
        f"Acervo carregado: tiles={total_tiles}, cache={'hit' if cache_hit else 'miss'}.",
        arquivo=nome_arquivo_base,
    )

    tiles_catalogo = [
        TileInfo(path=path, average_color=rgb, created_at=created_at)
        for path, rgb, created_at in descritores_catalogo
    ]

    if ENABLE_TILE_PRELOAD:
        qtd_preload, tempo_preload, preload_hit = _preaquecer_cache_tiles_renderizados(
            descritores_catalogo,
            tamanho_final_pixel,
        )
    else:
        qtd_preload, tempo_preload, preload_hit = 0, 0.0, False

    progresso_indexacao.update(2)
    if not tiles_catalogo:
        progresso_indexacao.close()
        terminal_log(
            "Erro final no processamento: acervo sem tiles validos.",
            level="ERROR",
            arquivo=nome_arquivo_base,
        )
        raise ValueError("Acervo sem tiles válidos no index SQLite.")

    buckets_por_cor = _construir_buckets_por_cor(tiles_catalogo)
    progresso_indexacao.update(3)
    progresso_indexacao.close()

    production_log(f"CATALOGO_TILES_RAM={len(tiles_catalogo)}")
    production_log(f"TOTAL_BUCKETS_RAM={len(buckets_por_cor)}")
    production_log(f"TILES_PRELOAD_RAM={qtd_preload}")
    production_log(f"TILES_PRELOAD_CACHE_HIT={preload_hit}")
    production_log(f"TEMPO_PRELOAD_TILES_S={tempo_preload:.3f}")
    production_log(f"TILES_PRELOAD_ENABLED={ENABLE_TILE_PRELOAD}")

    tempo_carregamento_acervo = time.perf_counter() - t_carregamento_acervo
    production_log(f"TEMPO_CARREGAR_ACERVO_S={tempo_carregamento_acervo:.3f}")
    terminal_log(f"Etapa carregamento_acervo: {tempo_carregamento_acervo:.2f}s.", arquivo=nome_arquivo_base)
    terminal_log("Carregamento do acervo concluido.", arquivo=nome_arquivo_base)

    largura_final = colunas * tamanho_final_pixel
    altura_final = linhas * tamanho_final_pixel

    cor_anterior = None
    metricas_tempo: Dict[str, float] = {
        "selecao_tiles_s": 0.0,
        "composicao_final_s": 0.0,
        "exportacao_jpeg_s": 0.0,
    }

    total_celulas = colunas * linhas
    terminal_log(f"Montagem iniciada. Total de celulas: {total_celulas}.", arquivo=nome_arquivo_base)
    t_montagem = time.perf_counter()
    progresso_montagem = TerminalProgress("Montagem do mosaico", total_celulas, arquivo=nome_arquivo_base)
    progresso_montagem.start()

    def _callback_progresso_montagem(celula_atual: int, total: int) -> None:
        progresso_montagem.update(celula_atual)
        if callback_progresso:
            callback_progresso(celula_atual, total)

    try:
        if usar_bandas:
            linhas_por_banda = max(1, int(linhas_por_banda))
            faixas = _iterar_faixas(linhas, linhas_por_banda)
            production_log(f"TOTAL_FAIXAS={len(faixas)}")

            writer = _MosaicMmapWriter(largura_final, altura_final)
            try:
                for faixa_idx, (linha_inicio, linha_fim) in enumerate(faixas):
                    debug_log(f"FAIXA_INICIO={linha_inicio} FAIXA_FIM={linha_fim}")
                    faixa_img, cor_anterior = _renderizar_faixa(
                        linha_inicio,
                        linha_fim,
                        colunas,
                        linhas,
                        tamanho_final_pixel,
                        img_redimensionada,
                        variacao_cor_efetiva,
                        max_repeticoes_efetivo,
                        buckets_por_cor,
                        tiles_catalogo,
                        ultimas_posicoes,
                        cor_anterior,
                        _callback_progresso_montagem,
                        cache_candidatos_por_cor,
                        metricas_tempo,
                    )

                    writer.write_strip(linha_inicio * tamanho_final_pixel, faixa_img)
                    faixa_img.close()

                    if DEBUG_VERBOSE_CELLS:
                        debug_log(f"FAIXA_ESCRITA={faixa_idx}")

                progresso_montagem.close()
                terminal_log("Montagem concluida.", arquivo=nome_arquivo_base)
                terminal_log("Inicio da gravacao final do mosaico.", arquivo=nome_arquivo_base)
                t_export = time.perf_counter()
                writer.save_jpeg(caminho_saida, qualidade)
                metricas_tempo["exportacao_jpeg_s"] = time.perf_counter() - t_export
                production_log(f"TEMPO_SALVAR_JPEG_S={metricas_tempo['exportacao_jpeg_s']:.3f}")
                terminal_log("Exportacao concluida.", arquivo=nome_arquivo_base)
            finally:
                writer.close()
        else:
            mosaico = Image.new("RGB", (largura_final, altura_final), "white")
            try:
                faixa_img, cor_anterior = _renderizar_faixa(
                    0,
                    linhas,
                    colunas,
                    linhas,
                    tamanho_final_pixel,
                    img_redimensionada,
                    variacao_cor_efetiva,
                    max_repeticoes_efetivo,
                    buckets_por_cor,
                    tiles_catalogo,
                    ultimas_posicoes,
                    cor_anterior,
                    _callback_progresso_montagem,
                    cache_candidatos_por_cor,
                    metricas_tempo,
                )
                mosaico.paste(faixa_img, (0, 0))
                faixa_img.close()
                progresso_montagem.close()
                terminal_log("Montagem concluida.", arquivo=nome_arquivo_base)
                terminal_log("Inicio da gravacao final do mosaico (AVIF).", arquivo=nome_arquivo_base)
                t_export = time.perf_counter()
                mosaico.save(caminho_saida, "AVIF", quality=30)
                metricas_tempo["exportacao_avif_s"] = time.perf_counter() - t_export
                production_log(f"TEMPO_SALVAR_AVIF_S={metricas_tempo['exportacao_avif_s']:.3f}")
                terminal_log("Exportacao concluida (AVIF).", arquivo=nome_arquivo_base)
            finally:
                mosaico.close()
    except Exception as exc:
        progresso_montagem.close()
        terminal_log(f"Erro final no processamento: {exc}", level="ERROR", arquivo=nome_arquivo_base)
        raise

    cache_info = _carregar_tile_renderizado.cache_info()
    production_log(
        f"TILE_LRU_CACHE hits={cache_info.hits} misses={cache_info.misses} "
        f"currsize={cache_info.currsize} maxsize={cache_info.maxsize}"
    )
    production_log(f"GRID_CORES_UNICAS={len(cache_candidatos_por_cor)}")
    production_log(f"TEMPO_LEITURA_BASE_S={tempo_leitura_base:.3f}")
    production_log(f"TEMPO_GERACAO_GRID_S={tempo_calculo_grid:.3f}")
    production_log(f"TEMPO_SELECAO_TILES_S={metricas_tempo['selecao_tiles_s']:.3f}")
    production_log(f"TEMPO_COMPOSICAO_FINAL_S={metricas_tempo['composicao_final_s']:.3f}")
    production_log(f"TEMPO_EXPORTACAO_JPEG_S={metricas_tempo['exportacao_jpeg_s']:.3f}")
    production_log(f"TEMPO_SELECAO_E_COMPOSICAO_S={time.perf_counter() - t_montagem:.3f}")

    terminal_log(f"Etapa selecao_tiles: {metricas_tempo['selecao_tiles_s']:.2f}s.", arquivo=nome_arquivo_base)
    terminal_log(f"Etapa composicao_final: {metricas_tempo['composicao_final_s']:.2f}s.", arquivo=nome_arquivo_base)
    terminal_log(f"Etapa exportacao_jpeg: {metricas_tempo['exportacao_jpeg_s']:.2f}s.", arquivo=nome_arquivo_base)

    tempo_total = time.perf_counter() - inicio_execucao
    production_log(f"ARQUIVO_SAIDA={caminho_saida}")
    production_log(f"DIMENSAO_FINAL={largura_final}x{altura_final}")
    production_log(f"TEMPO_PRODUCAO_S={tempo_total:.3f}")
    production_log("=== FIM PRODUCAO ===")

    terminal_log(
        f"Sucesso final: mosaico salvo em {caminho_saida} ({largura_final}x{altura_final}) em {tempo_total:.2f}s.",
        arquivo=nome_arquivo_base,
    )

    return largura_final, altura_final


def calcular_tamanho_final(
    caminho_base: str,
    pasta_pixels: str,
    tamanho_pixel: int,
    redimensionar: bool
) -> Tuple[int, int, str]:
    """
    Calcula o tamanho final do mosaico sem criar a imagem.

    Returns:
        Tupla (largura, altura, descricao)
    """
    with Image.open(caminho_base) as img:
        largura_base = img.width
        altura_base = img.height

    if largura_base < MIN_BASE_WIDTH:
        return 0, 0, "A imagem base deve ter no mínimo 1000 px de largura."

    colunas = FIXED_COLUMNS
    linhas = FIXED_COLUMNS
    largura_final = FIXED_OUTPUT_WIDTH
    altura_final = linhas * FIXED_TILE_SIZE

    desc = (
        f"Configuração fixa: {FIXED_COLUMNS} colunas, tiles {FIXED_TILE_SIZE}x{FIXED_TILE_SIZE}px, "
        f"variação de cor {FIXED_COLOR_VARIATION}, repetição máxima {FIXED_MAX_REPETITIONS}. "
        f"Resultado: {largura_final}x{altura_final}px"
    )

    return largura_final, altura_final, desc