# server.py

from flask import Flask, request, jsonify, send_from_directory
from werkzeug.utils import secure_filename
import os, json, datetime, uuid
import threading
import time
from PIL import Image, UnidentifiedImageError
from pillow_heif import register_heif_opener

register_heif_opener()

app = Flask(__name__)

# Diretórios
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(ROOT_DIR, "input")
OUTPUT_DIR = os.path.join(ROOT_DIR, "Output")
JOBS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jobs.json")

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

ALLOWED_UPLOAD_EXTENSIONS = {".jpg", ".jpeg", ".heic", ".heif"}

_JOBS_LOCK = threading.Lock()
_JOBS_CACHE = None
_JOBS_CACHE_MTIME = None

def load_jobs():
    global _JOBS_CACHE
    global _JOBS_CACHE_MTIME

    with _JOBS_LOCK:
        if not os.path.exists(JOBS_PATH):
            tmp = JOBS_PATH + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({}, f)
            os.replace(tmp, JOBS_PATH)

        mtime = os.path.getmtime(JOBS_PATH)
        if _JOBS_CACHE is not None and _JOBS_CACHE_MTIME == mtime:
            return dict(_JOBS_CACHE)

        with open(JOBS_PATH, "r", encoding="utf-8") as f:
            jobs = json.load(f)

        _JOBS_CACHE = jobs
        _JOBS_CACHE_MTIME = mtime
        return dict(jobs)

def save_jobs(jobs):
    global _JOBS_CACHE
    global _JOBS_CACHE_MTIME

    with _JOBS_LOCK:
        tmp = JOBS_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(jobs, f, indent=2, ensure_ascii=False)
        os.replace(tmp, JOBS_PATH)
        _JOBS_CACHE = dict(jobs)
        _JOBS_CACHE_MTIME = os.path.getmtime(JOBS_PATH)

def generate_base_name():
    # mosaico_YYYYMMDD_HHMMSS_<rand>.jpg
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    rand = str(uuid.uuid4())[:6]
    return f"mosaico_{now}_{rand}.jpg"

@app.route("/upload", methods=["POST"])
def upload():
    t_upload_inicio = time.perf_counter()

    # 1) valida arquivo
    if "file" not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "Arquivo sem nome"}), 400

    nome_original_bruto = file.filename.lower()
    ext = os.path.splitext(nome_original_bruto)[1]
    if ext not in ALLOWED_UPLOAD_EXTENSIONS:
        return jsonify({"error": "Envie apenas arquivos JPG, JPEG, HEIC ou HEIF."}), 400

    jobs = load_jobs()

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_original = secure_filename(file.filename)

    # Nome interno único que será USADO TANTO NO INPUT QUANTO NO OUTPUT
    nome_arquivo = generate_base_name()

    input_path = os.path.join(INPUT_DIR, nome_arquivo)
    output_path = os.path.join(OUTPUT_DIR, nome_arquivo)

    # Normaliza internamente para JPEG para preservar o pipeline atual.
    t_normalizacao = time.perf_counter()
    try:
        file.stream.seek(0)
        with Image.open(file.stream) as img:
            rgb = img.convert("RGB")
            rgb.save(input_path, format="JPEG", quality=95)
    except UnidentifiedImageError:
        return jsonify({"error": "Arquivo de imagem inválido ou corrompido."}), 400
    except Exception:
        return jsonify({"error": "Não foi possível ler a imagem enviada."}), 400

    job_id = os.path.splitext(nome_arquivo)[0]  # base sem .jpg

    jobs[job_id] = {
        "job_id": job_id,
        "timestamp_criacao": timestamp,
        "nome_original": nome_original,
        "nome_arquivo": nome_arquivo,    # mesmo nome em input e output
        "status": "recebido",
        "erro": None,
        "output_url": None,
        "input_path": input_path,
        "output_path": output_path,
        "timings": {
            "criacao_job_s": round(time.perf_counter() - t_upload_inicio, 3),
            "normalizacao_upload_s": round(time.perf_counter() - t_normalizacao, 3),
        },
    }

    t_save_jobs = time.perf_counter()
    save_jobs(jobs)
    app.logger.info(
        "[job=%s] criacao_job=%.3fs normalizacao_upload=%.3fs update_jobs_json=%.3fs",
        job_id,
        time.perf_counter() - t_upload_inicio,
        time.perf_counter() - t_normalizacao,
        time.perf_counter() - t_save_jobs,
    )

    return jsonify({"ok": True, "job_id": job_id})

@app.route("/status/<job_id>", methods=["GET"])
def status(job_id):
    jobs = load_jobs()
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job não encontrado"}), 404

    # se o arquivo com mesmo nome apareceu no Output, está pronto
    alterou = False
    if os.path.exists(job["output_path"]):
        output_url = f"/output/{job['nome_arquivo']}"
        if job.get("status") != "pronto" or job.get("output_url") != output_url or job.get("erro"):
            job["status"] = "pronto"
            job["output_url"] = output_url
            job["erro"] = None
            jobs[job_id] = job
            alterou = True
    elif job.get("erro"):
        job["status"] = "erro"
        jobs[job_id] = job
        alterou = True

    if alterou:
        save_jobs(jobs)

    return jsonify(
        {
            "job_id": job["job_id"],
            "status": job["status"],
            "erro": job["erro"],
            "output_url": job["output_url"],
            "nome_original": job.get("nome_original"),
        }
    )

@app.route("/output/<filename>")
def serve_output(filename):
    return send_from_directory(OUTPUT_DIR, filename)

@app.route("/")
def home():
    return send_from_directory(".", "index.html")

@app.route("/transicao.html")
def transicao():
    return send_from_directory(".", "transicao.html")

@app.route("/<path:path>")
def static_files(path):
    return send_from_directory(".", path)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5500, debug=True)