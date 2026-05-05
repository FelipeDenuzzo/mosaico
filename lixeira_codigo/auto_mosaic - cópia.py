import os
import shutil
import time
import logging
import json
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import sys
# Adiciona o diretório atual ao path para poder importar o mosaico.py
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mosaico import criar_mosaico

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

# ==========================================
# CONFIGURAÇÕES DO MOSAICO
# ==========================================
PASTA_ACERVO_TILES = "/Users/felipedenuzzo/VSCODE/Mosaico Programas/acervo"
TILES_BASE = PASTA_ACERVO_TILES

# Categoria de tiles para a automação (Informacao, Medicamentos, Pornografia, Geral)
TILES_CATEGORIA = "Informacao"
TILE_SIZE = 30
MAX_USES = 2
COLOR_VARIATION = 20
MAX_CONCURRENT_JOBS = max(1, int(os.getenv("MOSAICO_MAX_CONCURRENT_JOBS", "1")))

# Inicializa o Logger
LOG_FILE = os.path.join(BASE_DIR, "watcher_log.txt")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Adiciona log no console também
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# Tira o prefixo se quiser mais limpo na tela, mas vamos manter igual ao arquivo
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%H:%M:%S"))
logging.getLogger("").addHandler(console)

COUNTER_LOCK = threading.Lock()
ACTIVE_JOBS_LOCK = threading.Lock()
ACTIVE_INPUT_PATHS: set[str] = set()
JOBS_LOCK = threading.Lock()
JOB_TIMINGS_LOCK = threading.Lock()
JOB_TIMINGS_BUFFER: dict[str, dict[str, float]] = {}
JOB_WORKER_POOL: ThreadPoolExecutor | None = None
STUCK_JOB_TTL_SECONDS = max(60, int(os.getenv("MOSAICO_STUCK_JOB_TTL_SECONDS", "900")))


def _timestamp_now() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _split_ext(filename: str) -> str:
    ext = os.path.splitext(filename)[1].lower()
    return ext if ext else ".bin"


def _is_job_named_input(filename: str) -> bool:
    return filename.startswith("mosaico_") and "_input." in filename


def _job_id(timestamp_str: str, code: str) -> str:
    return f"mosaico_{timestamp_str}_{code}"


def _make_input_name(timestamp_str: str, code: str, ext: str) -> str:
    return f"mosaico_{timestamp_str}_{code}_input{ext}"


def _make_output_name(timestamp_str: str, code: str, ext: str = ".jpg") -> str:
    return f"mosaico_{timestamp_str}_{code}_output{ext}"


def _make_meta_name(timestamp_str: str, code: str) -> str:
    return f"mosaico_{timestamp_str}_{code}_meta.json"


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


def _next_job_code() -> str:
    with COUNTER_LOCK:
        atual = _read_counter_value()
        prox = atual + 1
        if prox > 999999:
            prox = 1
        _write_counter_value(prox)
    return f"{prox:06d}"


def _path_conflicts(*paths: str) -> bool:
    return any(os.path.exists(path) for path in paths)


def _allocate_internal_names(original_filename: str) -> tuple[str, str, str, str, str]:
    """Mantem basename imutavel: input_name == output_name == nome original do upload."""
    timestamp_str = _timestamp_now()
    code = _next_job_code()
    input_name = os.path.basename(original_filename)
    output_name = input_name
    job_id = os.path.splitext(input_name)[0]

    if _path_conflicts(
        os.path.join(PROCESSING_DIR, input_name),
        os.path.join(OUTPUT_DIR, output_name),
    ):
        raise RuntimeError(f"Conflito de nome detectado para basename imutavel: {input_name}")

    return job_id, timestamp_str, code, input_name, output_name


def _write_json(path: str, payload: dict) -> None:
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


<<<<<<< retorno-a60b126
=======
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


def _update_job_status(job_id: str, status: str, erro: str | None = None, extra: dict | None = None, flush_timings: bool = False) -> None:
    jobs = _load_jobs()
    job = jobs.get(job_id)
    if not job:
        return

    job["status"] = status
    job["erro"] = erro
    if extra:
        job.update(extra)

    if flush_timings:
        novos_tempos = _consume_job_timings(job_id)
        if novos_tempos:
            atuais = job.get("timings") if isinstance(job.get("timings"), dict) else {}
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

    atuais = job.get("timings") if isinstance(job.get("timings"), dict) else {}
    atuais.update(novos_tempos)
    job["timings"] = atuais
    jobs[job_id] = job
    _save_jobs(jobs)



>>>>>>> local
def _move_without_overwrite(src: str, dst: str) -> None:
    if os.path.exists(dst):
        raise FileExistsError(f"Destino ja existe: {dst}")
    os.replace(src, dst)


<<<<<<< retorno-a60b126
=======
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
# MANUTENÇÃO DE JOBS TEMPORÁRIOS
# ==========================================
>>>>>>> local
def cleanup_expired_job_dirs() -> None:
    """Remove pastas temporarias antigas para evitar acumulo no processing/jobs_tmp."""
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
                logging.info(f"[cleanup] pasta temporaria removida: {job_dir}")
        except Exception as e:
            logging.warning(f"[cleanup] falha ao limpar {job_dir}: {e}")


<<<<<<< retorno-a60b126
=======
def reconcile_stuck_jobs() -> None:
    """Marca jobs presos em processando por muito tempo como erro para evitar pendencias infinitas."""
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
            criado_em = datetime.strptime(ts, "%Y%m%d_%H%M%S")
        except Exception:
            continue

        idade = (agora - criado_em).total_seconds()
        if idade > STUCK_JOB_TTL_SECONDS:
            job["status"] = "erro"
            job["erro"] = "Job interrompido anteriormente e marcado como travado na inicializacao do watcher."
            jobs[job_id] = job
            alterou = True

    if alterou:
        _save_jobs(jobs)
        logging.info("[reconcile] Jobs travados em processando foram marcados como erro.")



>>>>>>> local
def ensure_dirs():
    """Garante que todas as pastas existam."""
    for d in [INPUT_DIR, PROCESSING_DIR, OUTPUT_DIR, ARCHIVE_DIR, ERROR_DIR, JOBS_TMP_DIR]:
        os.makedirs(d, exist_ok=True)


def wait_for_stability(filepath, check_interval=2, retries=30):
    """
    Aguarda até que o tamanho do arquivo pare de mudar.
    Evita processar imagens enquanto ainda estão sendo copiadas (pelo UX ou Finder).
    """
    previous_size = -1
    attempts = 0
    
    while attempts < retries:
        try:
            current_size = os.path.getsize(filepath)
            # Se o arquivo tem algum tamanho e ele parou de crescer
            if current_size > 0 and current_size == previous_size:
                return True
            previous_size = current_size
        except FileNotFoundError:
            return False # Arquivo sumiu / renomeado antes de estabilizar
            
        time.sleep(check_interval)
        attempts += 1
        
    return False


def process_image(src_path: str):
<<<<<<< retorno-a60b126
    """Fluxo de job com nomenclatura unica e rastreio por meta.json."""
    original_name = os.path.basename(src_path)
    if original_name.startswith("."):
        return

    logging.info(f"[{original_name}] Entrada detectada no input/.")

    # 1) Garantir upload completo antes de qualquer rename/move.
    if not wait_for_stability(src_path):
        logging.error(f"[{original_name}] Estabilizacao falhou (arquivo ainda em copia ou removido).")
        return

    if not os.path.exists(src_path):
        logging.warning(f"[{original_name}] Arquivo nao encontrado apos estabilizacao; ignorando evento.")
        return

    # 2) Preservar basename do upload para todo o fluxo.
    try:
        job_id, ts_str, code, input_name, output_name = _allocate_internal_names(original_name)
    except Exception as e:
        logging.error(f"[{original_name}] Falha ao gerar identificador interno: {e}")
        return

    internal_input_path = os.path.join(INPUT_DIR, input_name)
    internal_output_path = os.path.join(OUTPUT_DIR, output_name)
    job_dir = os.path.join(JOBS_TMP_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)

    try:
        if os.path.abspath(src_path) != os.path.abspath(internal_input_path):
            _move_without_overwrite(src_path, internal_input_path)
        logging.info(f"[{job_id}] Basename preservado no input: {input_name}")
    except Exception as e:
        logging.error(f"[{original_name}] Falha ao preparar input com basename imutavel: {e}")
        shutil.rmtree(job_dir, ignore_errors=True)
        return

    meta = {
        "job_id": job_id,
        "timestamp_criacao": ts_str,
        "nome_original": original_name,
        "nome_interno_input": input_name,
        "nome_interno_output": output_name,
        "status": "renomeado_input",
    }
    meta_path = os.path.join(job_dir, "meta.json")
    _write_json(meta_path, meta)

    # 3) Mover para processamento temporario por job.
    processing_input_path = os.path.join(job_dir, input_name)
    processing_output_path = os.path.join(job_dir, output_name)
    try:
        _move_without_overwrite(internal_input_path, processing_input_path)
    except Exception as e:
        meta["status"] = "erro_movendo_processing"
        meta["erro"] = str(e)
        _write_json(meta_path, meta)
        logging.error(f"[{job_id}] Falha ao mover para processamento temporario: {e}")
        return

    meta["status"] = "processando"
    _write_json(meta_path, meta)
    logging.info(f"[{job_id}] Inicio do processamento.")

    # 4) Chamar logica do mosaico sem alteracoes internas.
    sucesso = False
    erro_msg = ""
    try:
        w, h = criar_mosaico(
            caminho_base=processing_input_path,
            categoria=TILES_CATEGORIA,
            tamanho_pixel=TILE_SIZE,
            redimensionar=True,
            max_repeticoes=MAX_USES,
            variacao_cor=COLOR_VARIATION,
            caminho_saida=processing_output_path,
            usar_bandas=True,
            linhas_por_banda=5
        )
        sucesso = True
        logging.info(f"[{job_id}] Sucesso no mosaico: {w}x{h}.")
    except Exception as e:
        erro_msg = str(e)
        logging.error(f"[{job_id}] Erro no mosaico: {erro_msg}")

    # 5) Entregar output e arquivar input com vinculo pelo mesmo codigo.
    meta_file_name = _make_meta_name(ts_str, code)
    try:
        if sucesso:
            _move_without_overwrite(processing_output_path, internal_output_path)
            _move_without_overwrite(processing_input_path, os.path.join(ARCHIVE_DIR, input_name))
            meta["status"] = "sucesso"
            meta["output_path"] = internal_output_path
            _write_json(meta_path, meta)
            _move_without_overwrite(meta_path, os.path.join(ARCHIVE_DIR, meta_file_name))
            logging.info(f"[{job_id}] Entrega concluida: output={output_name} | input arquivado.")
        else:
            if os.path.exists(processing_input_path):
                _move_without_overwrite(processing_input_path, os.path.join(ERROR_DIR, input_name))
            if os.path.exists(processing_output_path):
                _move_without_overwrite(processing_output_path, os.path.join(ERROR_DIR, output_name))
            meta["status"] = "erro"
            meta["erro"] = erro_msg
            _write_json(meta_path, meta)
            _move_without_overwrite(meta_path, os.path.join(ERROR_DIR, meta_file_name))
            logging.info(f"[{job_id}] Job movido para error/.")
    except Exception as e:
        logging.error(f"[{job_id}] Falha na finalizacao/organizacao de arquivos: {e}")
=======
    """
    Fluxo de job:
    - estabiliza upload em input
    - preserva basename
    - copia o input em alta para archive antes de consumi-lo
    - processa em jobs_tmp
    - gera mosaico em Output
    """
    if not _try_acquire_input_slot(src_path):
        logging.info(f"[dedup] evento duplicado ignorado para {src_path}")
        return

    inicio_total = time.perf_counter()
    job_id = None

    try:
        original_name = os.path.basename(src_path)
        if original_name.startswith("."):
            return

        logging.info(f"[{original_name}] Inicio do job no watcher.")
        detectado_em = time.perf_counter() - inicio_total
        logging.info(f"[{original_name}] Etapa deteccao_watcher: {detectado_em:.2f}s.")


        # 1) Esperar upload completo
        t_estabilizacao = time.perf_counter()
        if not wait_for_stability(src_path):
            logging.error(f"[{original_name}] Estabilizacao falhou (arquivo ainda em copia ou removido).")
            return
        logging.info(f"[{original_name}] Estabilizacao concluida em {time.perf_counter() - t_estabilizacao:.2f}s.")


        if not os.path.exists(src_path):
            logging.warning(f"[{original_name}] Arquivo nao encontrado apos estabilizacao; ignorando evento.")
            return


        # 2) Gerar identificadores internos, mantendo basename
        try:
            job_id, ts_str, code, input_name, output_name = _allocate_internal_names(original_name)
        except Exception as e:
            logging.error(f"[{original_name}] Falha ao gerar identificador interno: {e}")
            return

        _add_job_timing(job_id, "deteccao_watcher_s", detectado_em)
        _update_job_status(job_id, "processando")


        internal_input_path = os.path.join(INPUT_DIR, input_name)
        internal_output_path = os.path.join(OUTPUT_DIR, output_name)
        job_dir = os.path.join(JOBS_TMP_DIR, job_id)
        os.makedirs(job_dir, exist_ok=True)


        # 3) Copiar para archive primeiro e só depois mover para o input interno
        t_preparacao = time.perf_counter()
        try:
            archive_input_path = os.path.join(ARCHIVE_DIR, input_name)

            if os.path.exists(archive_input_path):
                raise FileExistsError(f"Arquivo ja existe no archive: {archive_input_path}")

            t_copia_archive = time.perf_counter()
            shutil.copy2(src_path, archive_input_path)
            logging.info(f"[{job_id}] Copia concluida para archive: {archive_input_path}")
            tempo_copia_archive = time.perf_counter() - t_copia_archive
            logging.info(f"[{job_id}] Etapa copia_archive: {tempo_copia_archive:.2f}s.")
            _add_job_timing(job_id, "copia_archive_s", tempo_copia_archive)

            if os.path.abspath(src_path) != os.path.abspath(internal_input_path):
                _move_without_overwrite(src_path, internal_input_path)

            logging.info(f"[{job_id}] Basename preservado no input: {input_name}")

        except Exception as e:
            logging.error(f"[{original_name}] Falha ao copiar para archive e preparar input: {e}")
            shutil.rmtree(job_dir, ignore_errors=True)
            return
        logging.info(f"[{job_id}] Preparacao de arquivos concluida em {time.perf_counter() - t_preparacao:.2f}s.")
        _add_job_timing(job_id, "preparacao_job_tmp_s", time.perf_counter() - t_preparacao)


        meta = {
            "job_id": job_id,
            "timestamp_criacao": ts_str,
            "nome_original": original_name,
            "nome_interno_input": input_name,
            "nome_interno_output": output_name,
            "status": "renomeado_input",
        }
        meta_path = os.path.join(job_dir, "meta.json")
        _write_json(meta_path, meta)


        # 4) Mover para processamento temporário por job
        processing_input_path = os.path.join(job_dir, input_name)
        processing_output_path = os.path.join(job_dir, output_name)
        try:
            _move_without_overwrite(internal_input_path, processing_input_path)
        except Exception as e:
            meta["status"] = "erro_movendo_processing"
            meta["erro"] = str(e)
            _write_json(meta_path, meta)
            logging.error(f"[{job_id}] Falha ao mover para processamento temporario: {e}")
            return


        meta["status"] = "processando"
        _write_json(meta_path, meta)
        logging.info(f"[{job_id}] Inicio do processamento.")


        # 5) Chamar lógica do mosaico
        sucesso = False
        erro_msg = ""
        t_montagem = time.perf_counter()
        logging.info(f"[{job_id}] Inicio real da montagem do mosaico.")
        try:
            t_mosaico = time.perf_counter()
            w, h = criar_mosaico(
                caminho_base=processing_input_path,
                categoria=TILES_CATEGORIA,
                tamanho_pixel=TILE_SIZE,
                redimensionar=True,
                max_repeticoes=MAX_USES,
                variacao_cor=COLOR_VARIATION,
                caminho_saida=processing_output_path,
                usar_bandas=True,
                linhas_por_banda=5
            )
            sucesso = True
            tempo_montagem = time.perf_counter() - t_montagem
            logging.info(f"[{job_id}] Fim da montagem do mosaico em {tempo_montagem:.2f}s: {w}x{h}.")
            _add_job_timing(job_id, "pipeline_mosaico_total_s", time.perf_counter() - t_mosaico)
        except Exception as e:
            erro_msg = str(e)
            logging.error(f"[{job_id}] Erro no mosaico: {erro_msg}")


        # 6) Entregar output
        t_finalizacao = time.perf_counter()
        meta_file_name = _make_meta_name(ts_str, code)
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
                        "output_url": f"/output/{output_name}",
                        "output_path": internal_output_path,
                    },
                    flush_timings=True,
                )
                logging.info(f"[{job_id}] Entrega concluida: output={output_name}.")

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
            logging.error(f"[{job_id}] Falha na finalizacao/organizacao de arquivos: {e}")
        finally:
            tempo_finalizacao = time.perf_counter() - t_finalizacao
            logging.info(f"[{job_id}] Finalizacao concluida em {tempo_finalizacao:.2f}s.")
            _add_job_timing(job_id, "finalizacao_s", tempo_finalizacao)
            shutil.rmtree(job_dir, ignore_errors=True)
            cleanup_expired_job_dirs()
            tempo_total = time.perf_counter() - inicio_total
            _add_job_timing(job_id, "tempo_total_job_s", tempo_total)
            _flush_job_timings(job_id)
            logging.info(f"[{job_id}] Fim do job. Tempo total: {tempo_total:.2f}s.")
>>>>>>> local
    finally:
        _release_input_slot(src_path)


class InputFolderHandler(FileSystemEventHandler):
    """Event handler chamado pela lib watchdog"""
    def on_created(self, event):
        # Ignora a criação de pastas ou arquivos de sistema 
        if event.is_directory:
            return
            
        filename = os.path.basename(event.src_path)
        if filename.startswith("."):
            return
        # Protege contra reprocessar nomes internos que por algum motivo reaparecam em input.
        if _is_job_named_input(filename):
            return
<<<<<<< retorno-a60b126
            
        # Inicia numa thread separada para não travar o watcher em caso de inputs simultâneos
        t = threading.Thread(target=process_image, args=(event.src_path,))
        t.start()
=======

        if JOB_WORKER_POOL is None:
            logging.error("Pool de workers nao inicializado; evento ignorado.")
            return

        JOB_WORKER_POOL.submit(process_image, event.src_path)
>>>>>>> local


def start_watcher():
    global JOB_WORKER_POOL
    ensure_dirs()
    cleanup_expired_job_dirs()
<<<<<<< retorno-a60b126
    logging.info("="*50)
=======
    reconcile_stuck_jobs()
    JOB_WORKER_POOL = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS, thread_name_prefix="mosaic-job")
    logging.info("=" * 50)
>>>>>>> local
    logging.info("INICIANDO WATCHER AUTOMÁTICO DE MOSAICOS")
    logging.info(f"Monitorando -> {INPUT_DIR}")
    logging.info(f"Tiles (pixels) atuais -> {TILES_BASE}")
    logging.info(f"Tmp jobs -> {JOBS_TMP_DIR} (TTL {JOB_TMP_TTL_HOURS}h)")
    logging.info(f"Workers simultaneos -> {MAX_CONCURRENT_JOBS}")
    logging.info("Aguardando imagens novas pelo UX...")
    logging.info("="*50)
    
    event_handler = InputFolderHandler()
    observer = Observer()
    observer.schedule(event_handler, INPUT_DIR, recursive=False)
    
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Watcher interrompido pelo usuário (Ctrl+C).")
        observer.stop()
    observer.join()
    if JOB_WORKER_POOL is not None:
        JOB_WORKER_POOL.shutdown(wait=True, cancel_futures=False)
        JOB_WORKER_POOL = None


if __name__ == "__main__":
    start_watcher()
