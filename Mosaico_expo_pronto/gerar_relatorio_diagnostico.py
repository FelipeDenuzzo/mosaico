#!/usr/bin/env python3
"""
Gerador Unificado de Relatório de Diagnóstico - Mosaico EXPO
Varre todo o sistema, testa câmeras conectadas, verifica processos ativos,
audita o estado das pastas e unifica todos os logs em um único relatório para análise.
"""

import os
import sys
import time
import json
import sqlite3
import shutil
import subprocess
from datetime import datetime

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(ROOT_DIR, "logs")
OUTPUT_REPORT_PATH = os.path.join(ROOT_DIR, "RELATORIO_DIAGNOSTICO.txt")

def format_size(bytes_val):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_val < 1024:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.1f} TB"

def read_last_lines(filepath, num_lines=40):
    if not os.path.exists(filepath):
        return [f"[Arquivo de log não encontrado: {os.path.basename(filepath)}]\n"]
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
            return lines[-num_lines:]
    except Exception as e:
        return [f"[Erro ao ler log: {e}]\n"]

def classify_camera_device(name: str) -> tuple[bool, str]:
    n_lower = name.lower()
    if any(k in n_lower for k in ["integrated", "integrada", "internal", "embutida", "built-in", "front"]):
        return False, "CÂMERA INTEGRADA (ONBOARD)"
    return True, "WEBCAM USB EXTERNA"

def get_directshow_device_names():
    names = []
    if os.name == 'nt':
        try:
            cmd = (
                'powershell -NoProfile -Command "'
                '$path = \'Registry::HKEY_CLASSES_ROOT\\CLSID\\{860BB310-5D01-11BD-9B1A-00065BA000F4}\\Instance\'; '
                'if (Test-Path $path) { '
                '  Get-ChildItem $path | ForEach-Object { (Get-ItemProperty $_.PSPath).FriendlyName } '
                '}"'
            )
            res = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=5)
            if res.returncode == 0:
                for line in res.stdout.strip().splitlines():
                    if line.strip():
                        names.append(line.strip())
        except Exception:
            pass
    return names

def get_windows_pnp_cameras():
    cameras = []
    if os.name == 'nt':
        try:
            cmd = 'powershell -NoProfile -Command "Get-CimInstance Win32_PnPEntity | Where-Object { $_.PNPClass -in @(\'Camera\', \'Image\') } | ForEach-Object { $_.Name + \'|\' + $_.DeviceID }"'
            res = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=6)
            if res.returncode == 0:
                for line in res.stdout.strip().splitlines():
                    line = line.strip()
                    if '|' in line:
                        name, dev_id = line.split('|', 1)
                        is_usb, label = classify_camera_device(name)
                        cameras.append({"name": name, "device_id": dev_id, "is_usb": is_usb, "label": label})
        except Exception as e:
            cameras.append({"name": f"Erro ao consultar PowerShell: {e}", "device_id": "", "is_usb": False, "label": "DESCONHECIDO"})
    return cameras

def test_opencv_cameras():
    results = []
    try:
        import cv2
    except ImportError:
        return [{"error": "Biblioteca cv2 (OpenCV) não está instalada!"}]

    backends = [cv2.CAP_DSHOW, cv2.CAP_MSMF] if os.name == 'nt' else [cv2.CAP_ANY]

    for idx in range(5):
        found = False
        for backend in backends:
            backend_name = "DirectShow" if backend == cv2.CAP_DSHOW else ("MediaFoundation" if backend == cv2.CAP_MSMF else "ANY")
            try:
                cap = cv2.VideoCapture(idx, backend)
                if not cap.isOpened():
                    continue

                if backend == cv2.CAP_DSHOW:
                    cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*'MJPG'))
                cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
                cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)

                means = []
                stds = []
                last_frame = None

                for _ in range(8):
                    ret, frame = cap.read()
                    if ret and frame is not None and frame.size > 0:
                        means.append(frame.mean())
                        stds.append(frame.std())
                        last_frame = frame
                    time.sleep(0.06)

                cap.release()

                if last_frame is not None and len(means) > 0:
                    avg_m = sum(means[-4:]) / max(1, len(means[-4:]))
                    avg_s = sum(stds[-4:]) / max(1, len(stds[-4:]))
                    h, w = last_frame.shape[:2]
                    is_black = (avg_m < 2.0 and avg_s < 2.0)
                    results.append({
                        "index": idx,
                        "backend": backend_name,
                        "resolution": f"{w}x{h}",
                        "brightness": round(avg_m, 2),
                        "contrast": round(avg_s, 2),
                        "is_black": is_black,
                        "status": "IMAGEM PRETA (Sensor IR ou tampa fechada)" if is_black else "OK (Imagem colorida funcional)"
                    })
                    found = True
                    break
            except Exception as e:
                pass
        if not found:
            results.append({
                "index": idx,
                "backend": "Nenhum",
                "resolution": "N/A",
                "brightness": 0,
                "contrast": 0,
                "is_black": True,
                "status": "Não abriu (dispositivo ausente)"
            })
    return results

def get_running_processes():
    procs = []
    if os.name == 'nt':
        try:
            cmd = 'powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.Name -in @(\'python.exe\', \'node.exe\', \'msedge.exe\') } | Select-Object ProcessId, Name, CommandLine"'
            res = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=6)
            if res.returncode == 0:
                for line in res.stdout.strip().splitlines():
                    if line.strip():
                        procs.append(line.strip())
        except Exception as e:
            procs.append(f"Erro ao listar processos: {e}")
    else:
        try:
            cmd = "ps -ef | grep -E 'python|node' | grep -v grep"
            res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if res.returncode == 0:
                procs = [l.strip() for l in res.stdout.splitlines() if l.strip()]
        except Exception as e:
            procs.append(f"Erro ao listar processos: {e}")
    return procs

def audit_directory(dir_name):
    dir_path = os.path.join(ROOT_DIR, dir_name)
    if not os.path.exists(dir_path):
        return {"exists": False, "count": 0, "files": []}
    files = []
    try:
        for entry in os.scandir(dir_path):
            if entry.is_file() and not entry.name.startswith("."):
                stat = entry.stat()
                mtime = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                files.append(f"{entry.name} ({format_size(stat.st_size)}, {mtime})")
    except Exception as e:
        return {"exists": True, "count": 0, "files": [f"Erro: {e}"]}
    return {"exists": True, "count": len(files), "files": sorted(files, reverse=True)[:10]}

def generate_report():
    lines = []
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append("=" * 75)
    lines.append("RELATÓRIO UNIFICADO DE DIAGNÓSTICO - MOSAICO EXPO")
    lines.append(f"Data/Hora: {now_str}")
    lines.append(f"Pasta Raiz: {ROOT_DIR}")
    lines.append("=" * 75)
    lines.append("")

    # 1. Sistema & Ambiente
    lines.append("1. SISTEMA E AMBIENTE")
    lines.append(f"   - Sistema Operacional: {sys.platform} ({os.name})")
    lines.append(f"   - Python: {sys.version.split()[0]} ({sys.executable})")
    total, used, free = shutil.disk_usage(ROOT_DIR)
    lines.append(f"   - Espaço em Disco: Livre {format_size(free)} de {format_size(total)}")
    lines.append("")

    # 2. Câmeras no Windows (PnPEntity e DirectShow)
    lines.append("2. DISPOSITIVOS DE CÂMERA DETECTADOS NO SISTEMA")
    pnp_cams = get_windows_pnp_cameras()
    if pnp_cams:
        for c in pnp_cams:
            lines.append(f"   - [{c['label']}] {c['name']}")
            lines.append(f"     ID: {c['device_id']}")
    else:
        lines.append("   - Nenhuma câmera identificada via consulta PnPEntity.")
    lines.append("")

    dshow_names = get_directshow_device_names()
    if dshow_names:
        lines.append("   Mapeamento DirectShow (Ordem dos Índices no OpenCV):")
        for idx, dname in enumerate(dshow_names):
            _, label = classify_camera_device(dname)
            lines.append(f"     * Índice {idx}: '{dname}' [{label}]")
        lines.append("")

    # 3. Teste Físico OpenCV das Câmeras
    lines.append("3. TESTE DE IMAGEM E BRILHO DAS CÂMERAS (OpenCV)")
    cam_results = test_opencv_cameras()
    rec_index = None
    for res in cam_results:
        if "error" in res:
            lines.append(f"   - [ERRO] {res['error']}")
            continue
        dev_label = ""
        if dshow_names and res["index"] < len(dshow_names):
            dev_name = dshow_names[res["index"]]
            _, lab = classify_camera_device(dev_name)
            dev_label = f" [{lab}: '{dev_name}']"
        status_tag = "[ALERTA - PRETO]" if res["is_black"] and res["resolution"] != "N/A" else ("[OK]" if not res["is_black"] else "[INATIVO]")
        lines.append(f"   {status_tag} Índice {res['index']} ({res['backend']}){dev_label}: {res['resolution']} | Brilho: {res['brightness']} | {res['status']}")
        if not res["is_black"]:
            if dshow_names and res["index"] < len(dshow_names):
                is_u, _ = classify_camera_device(dshow_names[res["index"]])
                if is_u and rec_index is None:
                    rec_index = res["index"]
            elif rec_index is None:
                rec_index = res["index"]

    if rec_index is not None:
        rec_name = f" ('{dshow_names[rec_index]}')" if dshow_names and rec_index < len(dshow_names) else ""
        lines.append(f"\n   >>> WEBCAM USB EXTERNA RECOMENDADA: Índice {rec_index}{rec_name} <<<")
    else:
        lines.append("\n   >>> ATENÇÃO: Nenhuma câmera retornou imagem clara. Verifique cabo USB, tampa ou permissões.")
    lines.append("")

    # 4. Configurações do Projeto
    lines.append("4. CONFIGURAÇÕES E ESTADO ATUAL")
    cfg_path = os.path.join(ROOT_DIR, "camera_config.json")
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                lines.append(f"   - camera_config.json: {f.read().strip()}")
        except Exception as e:
            lines.append(f"   - camera_config.json: Erro: {e}")
    else:
        lines.append("   - camera_config.json: Não encontrado")

    man_path = os.path.join(ROOT_DIR, "manifest.json")
    if os.path.exists(man_path):
        try:
            with open(man_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                lines.append(f"   - manifest.json: {len(data.get('mosaics', []))} na parede, {len(data.get('queue', []))} na fila, isBusy={data.get('isBusy')}")
        except Exception as e:
            lines.append(f"   - manifest.json: Erro: {e}")

    db_path = os.path.join(ROOT_DIR, "tiles_index.db")
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM tiles")
            cnt = cur.fetchone()[0]
            conn.close()
            lines.append(f"   - tiles_index.db: {cnt} tiles indexados")
        except Exception as e:
            lines.append(f"   - tiles_index.db: Erro: {e}")
    lines.append("")

    # 5. Auditoria de Pastas do Fluxo
    lines.append("5. AUDITORIA DE PASTAS DO PROCESSO")
    for d in ["input", "Output", "processing", "error", "archive", "acervo"]:
        info = audit_directory(d)
        if not info["exists"]:
            lines.append(f"   - {d}/: NÃO EXISTE")
        else:
            lines.append(f"   - {d}/: {info['count']} arquivos")
            for f in info["files"][:4]:
                lines.append(f"       * {f}")
    lines.append("")

    # 6. Processos Ativos
    lines.append("6. PROCESSOS EM EXECUÇÃO")
    procs = get_running_processes()
    if procs:
        for p in procs[:15]:
            lines.append(f"   {p}")
    else:
        lines.append("   - Nenhum processo Python ou Node detectado em execução.")
    lines.append("")

    # 7. Logs Unificados dos Serviços
    lines.append("=" * 75)
    lines.append("7. LOGS UNIFICADOS (Últimas linhas de cada serviço)")
    lines.append("=" * 75)

    log_files = [
        ("CÂMERA (captura.log)", "captura.log"),
        ("WATCHER MOSAICO (auto_mosaic.log)", "auto_mosaic.log"),
        ("MOTOR MOSAICO (mosaico.log)", "mosaico.log"),
        ("WATCH MANIFEST (watch_manifest.log)", "watch_manifest.log"),
        ("INDEXADOR (indexador.log)", "indexador.log"),
        ("HTTP SERVER (serve_site.log)", "serve_site.log"),
    ]

    for title, fname in log_files:
        fpath = os.path.join(LOGS_DIR, fname)
        lines.append(f"\n--- {title} ---")
        log_content = read_last_lines(fpath, 30)
        for l in log_content:
            lines.append("  " + l.rstrip())

    lines.append("\n" + "=" * 75)
    lines.append("FIM DO RELATÓRIO DE DIAGNÓSTICO")
    lines.append("=" * 75)

    report_text = "\n".join(lines)
    try:
        with open(OUTPUT_REPORT_PATH, "w", encoding="utf-8") as f:
            f.write(report_text)
    except Exception as e:
        print(f"Erro ao salvar relatório: {e}")

    return report_text

if __name__ == "__main__":
    report = generate_report()
    print(report)
    print(f"\n>>> Relatório completo salvo em: {OUTPUT_REPORT_PATH} <<<")
