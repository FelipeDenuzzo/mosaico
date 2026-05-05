"""
Server Flask do Mosaico Online.
Recebe upload de imagem, salva em input/ e retorna job_id.
O processamento é feito pelo watcher (auto_mosaic.py).
"""
import os
import json
from pathlib import Path
from flask import Flask, jsonify, request


app = Flask(__name__)

BASE_DIR = Path("/Users/felipedenuzzo/VSCODE/Mosaico Programas")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "Output"
JOBS_PATH = BASE_DIR / "Site" / "jobs.json"

INPUT_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)


def _load_jobs() -> dict:
    try:
        if JOBS_PATH.exists():
            with open(JOBS_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


@app.post("/gerar")
def gerar():
    """
    Recebe upload de imagem e deposita em input/ para o watcher processar.
    Retorna o nome original para rastreamento do job.
    """
    if "imagem" not in request.files:
        return jsonify({"ok": False, "erro": "Campo 'imagem' ausente."}), 400

    arquivo = request.files["imagem"]
    if not arquivo.filename:
        return jsonify({"ok": False, "erro": "Nome de arquivo inválido."}), 400

    destino = INPUT_DIR / arquivo.filename
    if destino.exists():
        return jsonify({"ok": False, "erro": f"Arquivo '{arquivo.filename}' já está na fila."}), 409

    arquivo.save(destino)

    return jsonify({
        "ok": True,
        "mensagem": "Imagem recebida e na fila de processamento.",
        "nome_original": arquivo.filename,
    }), 202


@app.get("/status/<nome_original>")
def status(nome_original: str):
    """
    Consulta o status de um job pelo nome original enviado pelo UX.
    O job_id é derivado do basename sem extensão, limpo de espaços.
    """
    jobs = _load_jobs()

    # Procura pelo nome_original_ux registrado no job
    for job_id, job in jobs.items():
        if job.get("nome_original_ux") == nome_original:
            return jsonify(job)

    return jsonify({"ok": False, "erro": "Job não encontrado."}), 404


@app.get("/jobs")
def listar_jobs():
    """Retorna todos os jobs registrados."""
    return jsonify(_load_jobs())


if __name__ == "__main__":
    app.run(debug=True, port=5000)