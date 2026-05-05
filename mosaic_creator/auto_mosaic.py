"""
Watcher automático de mosaicos.
Monitora pasta input/, processa com mosaico.py e entrega em Output/.
Após job bem-sucedido, gera tile proporcional da imagem original e indexa no acervo.
Nome rastreado: {basename}_{HH}_{MM}_{DD}_{MM}_{AA}_{serial:06d}
"""
import os
import shutil
import time
import logging
import json
import sqlite3
from datetime import datetime
import importlib
import threading
from concurrent.futures import ThreadPoolExecutor
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from PIL import Image
import pillow_avif  # registra AVIF no Pillow
import pillow_heif  # registra HEIC/HEIF no Pillow

import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mosaico import criar_mosaico, INDEX_DB_PATH, invalidar_cache_catalogo

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'Index'))
indexador = importlib.import_module("indexador")
processar_pasta_temporaria = indexador.processar_pasta_temporaria
PASTA_TEMPORARIA_TILES = indexador.PASTA_TEMPORARIA_TILES


# ==========================================
# CONFIGURAÇÕES DE PASTAS
# ==========================================
BASE_DIR = "/Users/felipedenuzzo/VSCODE/Mosaico Programas"
INPUT_DIR = os.path.join(BASE_DIR, "input")
PROCESSING_DIR = os.path.join(BASE_DIR, "processing")
OUTPUT_DIR = os.path.join(BASE_DIR, "Output")
ARCHIVE_DIR = os.path.join(BASE_DIR, "archive")
ERROR_DIR = os.path.join(BASE_DIR, "error")
JOBS_TMP_DIR = os.path.join(PROCESSING_DIR, "jobs_tmp")
COUNTER_FILE = os.path.join(BASE_DIR, "job_counter.txt")
JOBS_PATH = os.path.join(BASE_DIR, "Site", "jobs.json")
JOB_TMP_TTL_HOURS = 24

USER_TILES_DIR = os.path.join(BASE_DIR, "acervo")


# ==========================================
# RESOLUÇÃO PROPORCIONAL DO TILE
# Quanto menor a foto original, maior o tile salvo no acervo
# para preservar qualidade visual no mosaico final.
#
# [ALTERADO 2026-05] TILE_BASE_SIZE dobrado para 60 (era 30) para melhorar projeção em parede
# Tiles maiores = melhor qualidade visual na exibição.
# ==========================================
TILE_BASE_SIZE = 60          # tamanho mínimo (tiles do acervo padrão) [dobrado]
TILE_MAX_SIZE = 300          # teto para evitar tiles gigantes
RESOLUCAO_REFERENCIA = 2000  # largura considerada "ideal" (fator 1.0)


# ==========================================
# CONFIGURAÇÕES DO MOSAICO
# ==========================================
TILE_SIZE = 100
MAX_USES = 2
COLOR_VARIATION = 20
MAX_CONCURRENT_JOBS = max(1, int(os.getenv("MOSAICO_MAX_CONCURRENT_JOBS", "1")))


# ==========================================
# LOGGING
# ==========================================
LOG_FILE = os.path.join(BASE_DIR, "watcher_log.txt")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%H:%M:%S"))
logging.getLogger("").addHandler(console)


# ==========================================
# LOCKS E ESTADO GLOBAL
# ==========================================
COUNTER_LOCK = threading.Lock()
ACTIVE_JOBS_LOCK = threading.Lock()
ACTIVE_INPUT_PATHS: set[str] = set()
JOBS_LOCK = threading.Lock()
JOB_TIMINGS_LOCK = threading.Lock()
JOB_TIMINGS_BUFFER: dict[str, dict[str, float]] = {}
JOB_WORKER_POOL: ThreadPoolExecutor | None = None
STUCK_JOB_TTL_SECONDS = max(60, int(os.getenv("MOSAICO_STUCK_JOB_TTL_SECONDS", "900")))


# ==========================================
# UTILITÁRIOS
# ==========================================
def _read_counter_value() -> int:
    if not os.path.exists(COUNTER_FILE):
        return 0
    try:
        with open(COUNTER_FILE, "r", encoding="utf-8") as f:
            return int((f.read() or "0").strip())
    except Exception:
        return 0


def _write_counter_value(value: int) -> None:
    tmp_path = f"{COUNTER_FILE}.{os.getpid()}.{threading.get_ident()}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(str(value))
    os.replace(tmp_path, COUNTER_FILE)


def _next_serial() -> str:
    with COUNTER_LOCK:
        atual = _read_counter_value()
        prox = atual + 1
        if prox > 999999:
            prox = 1
        _write_counter_value(prox)
    return f"{prox:06d}"


def _gerar_nome_rastreado(original_filename: str) -> str:
    basename = os.path.splitext(original_filename)[0]
    basename_limpo = basename.replace(" ", "")
    agora = datetime.now()
    parte_tempo = agora.strftime("%H_%M_%d_%m_%y")
    serial = _next_serial()
    return f"{basename_limpo}_{parte_tempo}_{serial}"


def _allocate_internal_names(original_filename: str) -> tuple[str, str, str, str]:
    extensao = os.path.splitext(original_filename)[1]
    nome_rastreado = _gerar_nome_rastreado(original_filename)
    job_id = nome_rastreado
    input_name = nome_rastreado + extensao
    # Sempre salvar o mosaico final em AVIF, independente da extensão de entrada
    output_name = nome_rastreado + ".avif"

    if os.path.exists(os.path.join(PROCESSING_DIR, input_name)) or \
       os.path.exists(os.path.join(OUTPUT_DIR, output_name)):
        raise RuntimeError(f"Conflito de nome rastreado: {input_name}")

    return job_id, nome_rastreado, input_name, output_name


def _calcular_tamanho_tile(largura_original: int) -> int:
    """
    Calcula o tamanho do tile proporcional à resolução da imagem original.

    Quanto menor a resolução enviada pelo UX, maior o tile gerado,
    preservando visibilidade proporcional no mosaico final.

    Fórmula:
        fator = min(largura_original / RESOLUCAO_REFERENCIA, 1.0)
        tamanho = clamp(30 / fator, TILE_BASE_SIZE, TILE_MAX_SIZE)
    """
    fator = min(largura_original / RESOLUCAO_REFERENCIA, 1.0)
    fator = max(fator, 0.01)  # evita divisão por zero
    tamanho = int(TILE_BASE_SIZE / fator)
    return max(TILE_BASE_SIZE, min(tamanho, TILE_MAX_SIZE))


def _write_json(path: str, payload: dict) -> None:
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def _load_jobs() -> dict:
    with JOBS_LOCK:
        if not os.path.exists(JOBS_PATH):
            _write_json(JOBS_PATH, {})
        try:
            with open(JOBS_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}


def _save_jobs(jobs: dict) -> None:
    with JOBS_LOCK:
        _write_json(JOBS_PATH, jobs)


def _consume_job_timings(job_id: str) -> dict:
    with JOB_TIMINGS_LOCK:
        return JOB_TIMINGS_BUFFER.pop(job_id, {})


def _update_job_status(
    job_id: str,
    status: str,
    erro: str | None = None,
    extra: dict | None = None,
    flush_timings: bool = False,
) -> None:
    jobs = _load_jobs()
    job = jobs.get(job_id)
    if not job:
        job = {"job_id": job_id, "status": status, "erro": erro, "timings": {}}
    else:
        job["status"] = status
        job["erro"] = erro

    if extra:
        job.update(extra)

    if flush_timings:
        novos_tempos = _consume_job_timings(job_id)
        if novos_tempos:
            atuais = job.get("timings", {})
            atuais.update(novos_tempos)
            job["timings"] = atuais

    jobs[job_id] = job
    _save_jobs(jobs)


def _add_job_timing(job_id: str, key: str, value_seconds: float) -> None:
    with JOB_TIMINGS_LOCK:
        bucket = JOB_TIMINGS_BUFFER.setdefault(job_id, {})
        bucket[key] = round(float(value_seconds), 3)


def _flush_job_timings(job_id: str) -> None:
    jobs = _load_jobs()
    job = jobs.get(job_id)
    if not job:
        _consume_job_timings(job_id)
        return

    novos_tempos = _consume_job_timings(job_id)
    if not novos_tempos:
        return

    atuais = job.get("timings", {})
    atuais.update(novos_tempos)
    job["timings"] = atuais
    jobs[job_id] = job
    _save_jobs(jobs)


def _move_without_overwrite(src: str, dst: str) -> None:
    if os.path.exists(dst):
        raise FileExistsError(f"Destino já existe: {dst}")
    os.replace(src, dst)


def _try_acquire_input_slot(src_path: str) -> bool:
    src_norm = os.path.abspath(src_path)
    with ACTIVE_JOBS_LOCK:
        if src_norm in ACTIVE_INPUT_PATHS:
            return False
        ACTIVE_INPUT_PATHS.add(src_norm)
    return True


def _release_input_slot(src_path: str) -> None:
    src_norm = os.path.abspath(src_path)
    with ACTIVE_JOBS_LOCK:
        ACTIVE_INPUT_PATHS.discard(src_norm)


# ==========================================
# CONVERSÃO DE ENTRADA PARA AVIF
# ==========================================

IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".jpe", ".jfif", ".jif",  # JPEG
    ".png",                                   # PNG
    ".heic", ".heif", ".hif",                # HEIC/HEIF (iPhone)
    ".webp",                                  # WebP
    ".avif",                                  # AVIF (se já vier pronto)
}

AVIF_INPUT_QUALITY = 80  # 75–85 é um bom intervalo


def _eh_imagem_suportada(path: str) -> bool:
    _, ext = os.path.splitext(path)
    return ext.lower() in IMAGE_EXTENSIONS


def _converter_entrada_para_avif(caminho_origem: str) -> str:
    """
    Converte uma imagem de entrada (HEIC/JPEG/PNG/WEBP/AVIF) para AVIF,
    salvando no mesmo diretório com a mesma base de nome.

    Retorna o caminho do arquivo AVIF gerado (ou original se já era AVIF
    ou se a conversão falhar).
    """
    if not _eh_imagem_suportada(caminho_origem):
        return caminho_origem

    base, ext = os.path.splitext(caminho_origem)
    destino = base + ".avif"

    # Se já existe AVIF ao lado, usa ele
    if os.path.exists(destino):
        return destino

    try:
        with Image.open(caminho_origem) as img:
            if img.mode not in ("RGB", "L"):
                img = img.convert("RGB")
            img.save(destino, "AVIF", quality=AVIF_INPUT_QUALITY)
        return destino
    except Exception as e:
        logging.warning(f"[converter_entrada_avif] Falha ao converter {caminho_origem} para AVIF: {e}")
        return caminho_origem


# ==========================================
# INTEGRAÇÃO COM INDEXADOR
# ==========================================
def integrar_mosaico_com_indexador(
    caminho_mosaico: str,
    job_id: str,
) -> bool:
    """
    Dispara o processamento do indexador para gerar tiles 100x100 do mosaico.
    O mosaico permanece em Output/ e NÃO é duplicado no archive.
    
    Retorna True se bem-sucedido, False caso contrário.
    """
    try:
        if not os.path.exists(caminho_mosaico):
            logging.warning(f"[{job_id}] Mosaico não encontrado para indexação: {caminho_mosaico}")
            return False
        
        # Processar o mosaico copiando temporariamente para o archive
        try:
            shutil.copy2(caminho_mosaico, PASTA_TEMPORARIA_TILES)
            with sqlite3.connect(INDEX_DB_PATH) as conn:
                processados = processar_pasta_temporaria(conn)
                if processados > 0:
                    invalidar_cache_catalogo()
                    nome_mosaico = os.path.basename(caminho_mosaico)
                    logging.info(
                        f"[{job_id}] Mosaico indexado e tile 100x100 gerado: {nome_mosaico}"
                    )
                else:
                    logging.warning(f"[{job_id}] Indexador não processou o mosaico.")
        except Exception as e:
            logging.warning(
                f"[{job_id}] Falha ao indexar mosaico: {e}"
            )
            return False
        
        return True
    
    except Exception as e:
        logging.warning(f"[{job_id}] Falha na integração com indexador: {e}")
        return False


# ==========================================
# INDEXAÇÃO DE TILE DO USUÁRIO
# ==========================================
def _calcular_rgb_medio(img: Image.Image) -> tuple[int, int, int]:
    pequena = img.convert("RGB").resize((1, 1), Image.Resampling.LANCZOS)
    return pequena.getpixel((0, 0))


def indexar_tile_usuario(
    caminho_original: str,
    nome_rastreado: str,
    job_id: str,
) -> bool:
    """
    Gera tile proporcional da imagem original, salva em acervo/ e indexa no banco.
    Tamanho do tile varia inversamente à resolução da imagem original.
    Após sucesso, remove imagem do archive (JSON permanece).
    """
    try:
        os.makedirs(USER_TILES_DIR, exist_ok=True)

        extensao = os.path.splitext(caminho_original)[1] or ".jpg"
        nome_tile = nome_rastreado + extensao
        caminho_tile = os.path.join(USER_TILES_DIR, nome_tile)

        with Image.open(caminho_original) as img:
            img_rgb = img.convert("RGB")
            largura_original = img_rgb.width

            tamanho_tile = _calcular_tamanho_tile(largura_original)

            # Crop quadrado centralizado
            w, h = img_rgb.size
            lado = min(w, h)
            left = (w - lado) // 2
            top = (h - lado) // 2
            img_rgb = img_rgb.crop((left, top, left + lado, top + lado))

            tile = img_rgb.resize((tamanho_tile, tamanho_tile), Image.Resampling.LANCZOS)
            r, g, b = _calcular_rgb_medio(tile)
            tile.save(caminho_tile, "JPEG", quality=90)


        # Calcula métricas adicionais do tile para compatibilidade com o schema do banco
        width, height = tile.size

        # Vetores verticais (3 faixas horizontais)
        v1_box = (0, 0, width, height // 3)
        v2_box = (0, height // 3, width, 2 * height // 3)
        v3_box = (0, 2 * height // 3, width, height)

        v1 = tile.crop(v1_box)
        v2 = tile.crop(v2_box)
        v3 = tile.crop(v3_box)

        v1_r, v1_g, v1_b = _calcular_rgb_medio(v1)
        v2_r, v2_g, v2_b = _calcular_rgb_medio(v2)
        v3_r, v3_g, v3_b = _calcular_rgb_medio(v3)

        # Vetores horizontais (3 faixas verticais)
        h1_box = (0, 0, width // 3, height)
        h2_box = (width // 3, 0, 2 * width // 3, height)
        h3_box = (2 * width // 3, 0, width, height)

        h1 = tile.crop(h1_box)
        h2 = tile.crop(h2_box)
        h3 = tile.crop(h3_box)

        h1_r, h1_g, h1_b = _calcular_rgb_medio(h1)
        h2_r, h2_g, h2_b = _calcular_rgb_medio(h2)
        h3_r, h3_g, h3_b = _calcular_rgb_medio(h3)

        agora_iso = datetime.now().isoformat(timespec="seconds")

        with sqlite3.connect(INDEX_DB_PATH) as conn:
            existente = conn.execute(
                "SELECT id FROM tiles WHERE path = ?", (caminho_tile,)
            ).fetchone()
            if not existente:
                agora_iso = datetime.now().isoformat(timespec="seconds")
                conn.execute(
                    """
                    INSERT INTO tiles (
                        categoria,
                        path,
                        filename,
                        r, g, b,
                        bucket,
                        width,
                        height,
                        v1_r, v1_g, v1_b,
                        v2_r, v2_g, v2_b,
                        v3_r, v3_g, v3_b,
                        h1_r, h1_g, h1_b,
                        h2_r, h2_g, h2_b,
                        h3_r, h3_g, h3_b,
                        created_at,
                        updated_at
                    ) VALUES (
                        ?, ?, ?,
                        ?, ?, ?,
                        ?,
                        ?, ?,
                        ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?,
                        ?, ?
                    )
                    """,
                    (
                        "geral",              # categoria
                        caminho_tile,         # path
                        nome_tile,            # filename
                        r, g, b,              # média global
                        "acervo",             # bucket
                        width,
                        height,
                        v1_r, v1_g, v1_b,
                        v2_r, v2_g, v2_b,
                        v3_r, v3_g, v3_b,
                        h1_r, h1_g, h1_b,
                        h2_r, h2_g, h2_b,
                        h3_r, h3_g, h3_b,
                        agora_iso,            # created_at
                        agora_iso,            # updated_at
                    ),
                )
                conn.commit()
                invalidar_cache_catalogo()
                logging.info(
                    f"[{job_id}] Tile indexado no acervo e cache invalidado: "
                    f"{nome_tile} ({width}x{height}) RGB=({r},{g},{b})"
                )

        logging.info(
            f"[{job_id}] Tile indexado: {nome_tile} "
            f"tamanho={tamanho_tile}px "
            f"resolucao_original={largura_original}px "
            f"RGB=({r},{g},{b})"
        )

        try:
            os.remove(caminho_original)
            logging.info(f"[{job_id}] Imagem original removida do archive.")
        except Exception as e:
            logging.warning(f"[{job_id}] Não foi possível remover do archive: {e}")

        return True

    except Exception as e:
        logging.warning(
            f"[{job_id}] Falha ao indexar tile do usuário: {e} | "
            f"nome_tile={locals().get('nome_tile')} | "
            f"caminho_tile={locals().get('caminho_tile')} | "
            f"rgb=({locals().get('r')},{locals().get('g')},{locals().get('b')})"
        )
        return False


# ==========================================
# MANUTENÇÃO
# ==========================================
def cleanup_expired_job_dirs() -> None:
    now_ts = time.time()
    ttl_seconds = JOB_TMP_TTL_HOURS * 3600
    if not os.path.isdir(JOBS_TMP_DIR):
        return

    for name in os.listdir(JOBS_TMP_DIR):
        job_dir = os.path.join(JOBS_TMP_DIR, name)
        if not os.path.isdir(job_dir):
            continue
        try:
            age = now_ts - os.path.getmtime(job_dir)
            if age > ttl_seconds:
                shutil.rmtree(job_dir, ignore_errors=True)
                logging.info(f"[cleanup] Pasta temporária removida: {job_dir}")
        except Exception as e:
            logging.warning(f"[cleanup] Falha ao limpar {job_dir}: {e}")


def reconcile_stuck_jobs() -> None:
    jobs = _load_jobs()
    if not jobs:
        return

    agora = datetime.now()
    alterou = False

    for job_id, job in jobs.items():
        if job.get("status") != "processando":
            continue

        ts = job.get("timestamp_criacao")
        try:
            criado_em = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")
        except Exception:
            continue

        idade = (agora - criado_em).total_seconds()
        if idade > STUCK_JOB_TTL_SECONDS:
            job["status"] = "erro"
            job["erro"] = "Job interrompido anteriormente e marcado como travado."
            jobs[job_id] = job
            alterou = True

    if alterou:
        _save_jobs(jobs)
        logging.info("[reconcile] Jobs travados foram marcados como erro.")


def ensure_dirs() -> None:
    for d in [INPUT_DIR, PROCESSING_DIR, OUTPUT_DIR, ARCHIVE_DIR, ERROR_DIR, JOBS_TMP_DIR, USER_TILES_DIR]:
        os.makedirs(d, exist_ok=True)

    os.makedirs(os.path.dirname(JOBS_PATH), exist_ok=True)
    if not os.path.exists(JOBS_PATH):
        _write_json(JOBS_PATH, {})


def wait_for_stability(filepath: str, check_interval: int = 2, retries: int = 30) -> bool:
    previous_size = -1
    attempts = 0

    while attempts < retries:
        try:
            current_size = os.path.getsize(filepath)
            if current_size > 0 and current_size == previous_size:
                return True
            previous_size = current_size
        except FileNotFoundError:
            return False

        time.sleep(check_interval)
        attempts += 1

    return False


# ==========================================
# PROCESSAMENTO DE JOB
# ==========================================
def process_image(src_path: str) -> None:
    if not _try_acquire_input_slot(src_path):
        logging.info(f"[dedup] Evento duplicado ignorado para {src_path}")
        return

    inicio_total = time.perf_counter()
    job_id = None

    try:
        original_name = os.path.basename(src_path)
        if original_name.startswith("."):
            return

        logging.info(f"[{original_name}] Início do job no watcher.")
        detectado_em = time.perf_counter() - inicio_total

        t_estabilizacao = time.perf_counter()
        if not wait_for_stability(src_path):
            logging.error(f"[{original_name}] Estabilização falhou.")
            return
        tempo_estabilizacao = time.perf_counter() - t_estabilizacao
        logging.info(f"[{original_name}] Estabilização concluída em {tempo_estabilizacao:.2f}s.")

        if not os.path.exists(src_path):
            logging.warning(f"[{original_name}] Arquivo não encontrado após estabilização.")
            return

        try:
            job_id, nome_rastreado, input_name, output_name = _allocate_internal_names(original_name)
        except Exception as e:
            logging.error(f"[{original_name}] Falha ao gerar nome rastreado: {e}")
            return

        logging.info(f"[{job_id}] Nome rastreado: {nome_rastreado}")

        _add_job_timing(job_id, "deteccao_watcher_s", detectado_em)
        _add_job_timing(job_id, "estabilizacao_s", tempo_estabilizacao)
        _update_job_status(job_id, "processando", extra={
            "timestamp_criacao": datetime.now().isoformat(timespec="seconds"),
            "nome_original_ux": original_name,
            "nome_rastreado": nome_rastreado,
        })

        internal_input_path = os.path.join(INPUT_DIR, input_name)
        internal_output_path = os.path.join(OUTPUT_DIR, output_name)
        job_dir = os.path.join(JOBS_TMP_DIR, job_id)
        os.makedirs(job_dir, exist_ok=True)

        t_preparacao = time.perf_counter()
        archive_input_path = os.path.join(ARCHIVE_DIR, input_name)
        try:
            if os.path.exists(archive_input_path):
                raise FileExistsError(f"Arquivo já existe no archive: {archive_input_path}")

            shutil.copy2(src_path, archive_input_path)
            logging.info(f"[{job_id}] Cópia concluída para archive: {input_name}")

            if os.path.abspath(src_path) != os.path.abspath(internal_input_path):
                _move_without_overwrite(src_path, internal_input_path)

        except Exception as e:
            logging.error(f"[{job_id}] Falha ao preparar arquivo: {e}")
            shutil.rmtree(job_dir, ignore_errors=True)
            _update_job_status(job_id, "erro", str(e), flush_timings=True)
            return

        tempo_preparacao = time.perf_counter() - t_preparacao
        _add_job_timing(job_id, "preparacao_job_tmp_s", tempo_preparacao)

        meta = {
            "job_id": job_id,
            "nome_original_ux": original_name,
            "nome_rastreado": nome_rastreado,
            "timestamp_criacao": datetime.now().isoformat(timespec="seconds"),
            "nome_interno_input": input_name,
            "nome_interno_output": output_name,
            "status": "renomeado_input",
        }
        meta_path = os.path.join(job_dir, "meta.json")
        _write_json(meta_path, meta)

        processing_input_path = os.path.join(job_dir, input_name)
        processing_output_path = os.path.join(job_dir, output_name)
        try:
            _move_without_overwrite(internal_input_path, processing_input_path)
        except Exception as e:
            meta["status"] = "erro_movendo_processing"
            meta["erro"] = str(e)
            _write_json(meta_path, meta)
            logging.error(f"[{job_id}] Falha ao mover para processamento: {e}")
            _update_job_status(job_id, "erro", str(e), flush_timings=True)
            return

        meta["status"] = "processando"
        _write_json(meta_path, meta)
        logging.info(f"[{job_id}] Início do processamento.")

        # Converte a entrada (HEIC/JPEG/PNG/WEBP/AVIF) para AVIF antes do mosaico
        processing_input_avif = _converter_entrada_para_avif(processing_input_path)
        if processing_input_avif != processing_input_path:
            logging.info(f"[{job_id}] Entrada convertida para AVIF: {os.path.basename(processing_input_avif)}")

        sucesso = False
        erro_msg = ""
        t_mosaico = time.perf_counter()
        try:
            w, h = criar_mosaico(
                caminho_base=processing_input_avif,
                tamanho_pixel=TILE_SIZE,
                redimensionar=True,
                max_repeticoes=MAX_USES,
                variacao_cor=COLOR_VARIATION,
                caminho_saida=processing_output_path,
                usar_bandas=True,
                linhas_por_banda=5,
            )
            sucesso = True
            tempo_mosaico = time.perf_counter() - t_mosaico
            logging.info(f"[{job_id}] Mosaico concluído em {tempo_mosaico:.2f}s: {w}x{h}.")
            _add_job_timing(job_id, "pipeline_mosaico_total_s", tempo_mosaico)
        except Exception as e:
            erro_msg = str(e)
            logging.error(f"[{job_id}] Erro no mosaico: {erro_msg}")

        t_finalizacao = time.perf_counter()
        meta_file_name = f"{nome_rastreado}_meta.json"
        try:
            if sucesso:
                _move_without_overwrite(processing_output_path, internal_output_path)
                meta["status"] = "sucesso"
                meta["output_path"] = internal_output_path
                _write_json(meta_path, meta)
                _move_without_overwrite(meta_path, os.path.join(ARCHIVE_DIR, meta_file_name))
                _update_job_status(
                    job_id,
                    "pronto",
                    None,
                    {
                        "output_url": f"/Output/{output_name}",
                        "output_path": internal_output_path,
                        "nome_rastreado": nome_rastreado,
                    },
                    flush_timings=True,
                )
                logging.info(f"[{job_id}] Entrega concluída: {output_name}")

                indexar_tile_usuario(archive_input_path, nome_rastreado, job_id)
                
                # Integrar mosaico com indexador: copiar e gerar tile 100x100
                integrar_mosaico_com_indexador(internal_output_path, job_id)

            else:
                if os.path.exists(processing_input_path):
                    _move_without_overwrite(processing_input_path, os.path.join(ERROR_DIR, input_name))
                if os.path.exists(processing_output_path):
                    _move_without_overwrite(processing_output_path, os.path.join(ERROR_DIR, output_name))
                meta["status"] = "erro"
                meta["erro"] = erro_msg
                _write_json(meta_path, meta)
                _move_without_overwrite(meta_path, os.path.join(ERROR_DIR, meta_file_name))
                _update_job_status(job_id, "erro", erro_msg, flush_timings=True)
                logging.info(f"[{job_id}] Job movido para error/.")
        except Exception as e:
            logging.error(f"[{job_id}] Falha na finalização: {e}")
        finally:
            tempo_finalizacao = time.perf_counter() - t_finalizacao
            _add_job_timing(job_id, "finalizacao_s", tempo_finalizacao)
            shutil.rmtree(job_dir, ignore_errors=True)
            cleanup_expired_job_dirs()
            tempo_total = time.perf_counter() - inicio_total
            _add_job_timing(job_id, "tempo_total_job_s", tempo_total)
            _flush_job_timings(job_id)
            logging.info(f"[{job_id}] Fim do job. Tempo total: {tempo_total:.2f}s.")
    finally:
        _release_input_slot(src_path)


# ==========================================
# WATCHER
# ==========================================
class InputFolderHandler(FileSystemEventHandler):
    def on_created(self, event) -> None:
        if event.is_directory:
            return
        filename = os.path.basename(event.src_path)
        if filename.startswith("."):
            return
        if JOB_WORKER_POOL is None:
            logging.error("Pool de workers não inicializado; evento ignorado.")
            return
        JOB_WORKER_POOL.submit(process_image, event.src_path)


def start_watcher() -> None:
    global JOB_WORKER_POOL
    ensure_dirs()
    cleanup_expired_job_dirs()
    reconcile_stuck_jobs()
    JOB_WORKER_POOL = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS, thread_name_prefix="mosaic-job")
    logging.info("=" * 50)
    logging.info("WATCHER AUTOMÁTICO DE MOSAICOS")
    logging.info(f"Monitorando -> {INPUT_DIR}")
    logging.info(f"Tmp jobs -> {JOBS_TMP_DIR} (TTL {JOB_TMP_TTL_HOURS}h)")
    logging.info(f"Workers simultâneos -> {MAX_CONCURRENT_JOBS}")
    logging.info(f"Acervo user tiles -> {USER_TILES_DIR}")
    logging.info("Aguardando imagens...")
    logging.info("=" * 50)

    event_handler = InputFolderHandler()
    observer = Observer()
    observer.schedule(event_handler, INPUT_DIR, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Watcher interrompido (Ctrl+C).")
        observer.stop()
    observer.join()
    if JOB_WORKER_POOL is not None:
        JOB_WORKER_POOL.shutdown(wait=True, cancel_futures=False)
        JOB_WORKER_POOL = None


if __name__ == "__main__":
    start_watcher()
