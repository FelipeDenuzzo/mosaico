import os
import sys
import time
import json
import subprocess
from datetime import datetime
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import cv2

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(BASE_DIR, "input")
CONFIG_PATH = os.path.join(BASE_DIR, "camera_config.json")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOGS_DIR, "captura.log")

STREAM_PORT = 8082
IDLE_TIMEOUT_DEFAULT = 600  # 10 minutos em segundos
CAPTURE_COOLDOWN_SEC = 8.0  # Tempo mínimo entre fotos consecutivas

# Estado compartilhado para streaming e status
_FRAME_LOCK = threading.Lock()
_LATEST_JPEG = None
_LAST_MOTION_TIME = time.time()
_LAST_CAPTURE_TIME = 0.0
_IS_CAMERA_UNLOCKED = True
_LOCK_REASON = "Inicializando"
_CAMERA_ACTIVE = False
_ACTIVE_CAMERA_INDEX = None
_ACTIVE_BACKEND = "N/A"
_ACTIVE_STREAM_CLIENTS = 0
_CLIENTS_LOCK = threading.Lock()

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def classify_camera_device(name: str) -> tuple[bool, str]:
    """Classifica se o dispositivo é webcam USB externa ou câmera integrada."""
    n_lower = name.lower()
    if any(k in n_lower for k in ["integrated", "integrada", "internal", "embutida", "built-in", "front"]):
        return False, "CÂMERA INTEGRADA (ONBOARD)"
    return True, "WEBCAM USB EXTERNA"

def get_directshow_device_names():
    """Consulta a lista de câmeras DirectShow na ordem exata de índices do Windows."""
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
    """Identifica dispositivos de camera no Windows via PowerShell PnPEntity."""
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
            log(f"Aviso ao consultar PnPEntity no Windows: {e}")
    return cameras

def is_camera_unlocked() -> tuple[bool, str]:
    global _LOCK_REASON
    # 1. Verificar se há arquivos na pasta input
    if os.path.exists(INPUT_DIR):
        try:
            files = [f for f in os.listdir(INPUT_DIR) if not f.startswith(".")]
            if len(files) > 0:
                reason = f"Arquivos pendentes na pasta input ({len(files)} arquivos)"
                _LOCK_REASON = reason
                return False, reason
        except Exception:
            pass

    # 2. Verificar manifest.json
    manifest_path = os.path.join(BASE_DIR, "manifest.json")
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                is_busy = data.get("isBusy", False)
                is_busy_timestamp = data.get("isBusyTimestamp", 0)
                queue = data.get("queue", [])
                
                # Watchdog para isBusy
                if is_busy and is_busy_timestamp > 0:
                    age_ms = time.time() * 1000 - is_busy_timestamp
                    if age_ms > 45000:
                        is_busy = False
                        
                if is_busy:
                    reason = "Mural ocupado exibindo mosaico (isBusy=true)"
                    _LOCK_REASON = reason
                    return False, reason
                
                # Se há itens na fila do manifest há menos de 60s, aguarda
                if len(queue) > 0:
                    reason = f"Mosaicos aguardando na fila de exibição ({len(queue)} itens)"
                    _LOCK_REASON = reason
                    return False, reason
        except Exception:
            pass

    # 3. Verificar jobs.json
    jobs_path = os.path.join(BASE_DIR, "jobs.json")
    if os.path.exists(jobs_path):
        try:
            with open(jobs_path, "r", encoding="utf-8") as f:
                jobs = json.load(f)
                agora_ts = time.time()
                for job_id, job in jobs.items():
                    if job.get("status") == "processando":
                        # Watchdog para jobs travados em processando há mais de 120s
                        t_criacao = job.get("timestamp_criacao")
                        job_stuck = False
                        if t_criacao:
                            try:
                                dt = datetime.fromisoformat(t_criacao)
                                if (agora_ts - dt.timestamp()) > 120:
                                    job_stuck = True
                            except Exception:
                                pass
                        if not job_stuck:
                            reason = f"Job em processamento: {job_id}"
                            _LOCK_REASON = reason
                            return False, reason
        except Exception:
            pass

    _LOCK_REASON = "Livre para disparo"
    return True, "Livre"

class CameraStreamHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        global _ACTIVE_STREAM_CLIENTS

        if self.path.startswith('/video_feed'):
            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Type', 'multipart/x-mixed-replace; boundary=frame')
            self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
            self.send_header('Pragma', 'no-cache')
            self.send_header('Expires', '0')
            self.end_headers()

            with _CLIENTS_LOCK:
                _ACTIVE_STREAM_CLIENTS += 1

            try:
                while _CAMERA_ACTIVE:
                    with _FRAME_LOCK:
                        frame_bytes = _LATEST_JPEG

                    if frame_bytes is not None:
                        self.wfile.write(b'--frame\r\n')
                        self.wfile.write(b'Content-Type: image/jpeg\r\n')
                        self.wfile.write(f'Content-Length: {len(frame_bytes)}\r\n\r\n'.encode('ascii'))
                        self.wfile.write(frame_bytes)
                        self.wfile.write(b'\r\n')
                    time.sleep(0.04)  # ~25 FPS estável
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                with _CLIENTS_LOCK:
                    _ACTIVE_STREAM_CLIENTS = max(0, _ACTIVE_STREAM_CLIENTS - 1)

        elif self.path.startswith('/status'):
            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Type', 'application/json')
            self.send_header('Cache-Control', 'no-cache')
            self.end_headers()

            now = time.time()
            idle_seconds = max(0.0, now - _LAST_MOTION_TIME)
            is_idle = idle_seconds >= IDLE_TIMEOUT_DEFAULT

            payload = {
                "active": _CAMERA_ACTIVE,
                "cameraIndex": _ACTIVE_CAMERA_INDEX,
                "cameraBackend": _ACTIVE_BACKEND,
                "unlocked": _IS_CAMERA_UNLOCKED,
                "lockReason": _LOCK_REASON,
                "isIdle": is_idle,
                "idleSeconds": round(idle_seconds, 1),
                "idleTimeoutSeconds": IDLE_TIMEOUT_DEFAULT
            }
            self.wfile.write(json.dumps(payload).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

def start_stream_server(port=STREAM_PORT):
    try:
        server = ThreadingHTTPServer(('0.0.0.0', port), CameraStreamHandler)
        log(f"Servidor de streaming ativo em http://127.0.0.1:{port}/video_feed")
        server.serve_forever()
    except Exception as e:
        log(f"Erro ao iniciar servidor de streaming: {e}")

def test_camera_device(idx, backend, backend_name):
    """Abre e avalia se o índice e backend fornecem imagem real (não-preta)."""
    try:
        cap = cv2.VideoCapture(idx, backend)
        if not cap.isOpened():
            return None, 0, 0, 0.0, "Não abriu"

        # Configurações para webcams USB modernas
        if backend == cv2.CAP_DSHOW:
            cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*'MJPG'))
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)

        # Descarte de até 10 frames de aquecimento do sensor
        valid_frames = 0
        mean_vals = []
        std_vals = []
        last_frame = None

        for _ in range(12):
            ret, frame = cap.read()
            if ret and frame is not None and frame.size > 0:
                valid_frames += 1
                m = frame.mean()
                s = frame.std()
                mean_vals.append(m)
                std_vals.append(s)
                last_frame = frame
            time.sleep(0.08)

        if valid_frames == 0 or last_frame is None:
            cap.release()
            return None, 0, 0, 0.0, "Falha ao ler frames"

        avg_mean = sum(mean_vals[-5:]) / max(1, len(mean_vals[-5:]))
        avg_std = sum(std_vals[-5:]) / max(1, len(std_vals[-5:]))
        h, w = last_frame.shape[:2]

        # Se a imagem é completamente preta (brilho < 2.0 e desvio < 2.0)
        if avg_mean < 2.0 and avg_std < 2.0:
            cap.release()
            return None, w, h, avg_mean, "Imagem 100% preta (sensor IR ou lente tampada)"

        return cap, w, h, avg_mean, "OK"
    except Exception as e:
        return None, 0, 0, 0.0, f"Exceção: {e}"

def open_best_camera(preferred_index=1):
    global _ACTIVE_CAMERA_INDEX, _ACTIVE_BACKEND
    log("=" * 60)
    log(f"INICIANDO DETECCAO DE CAMERA (Alvo: Webcam USB no Indice {preferred_index})")
    log("=" * 60)

    # 1. Consulta dispositivos físicos no Windows (apenas informativo para log)
    pnp_cams = get_windows_pnp_cameras()
    if pnp_cams:
        log("Dispositivos de video detectados pelo Windows:")
        for c in pnp_cams:
            log(f"  - [{c['label']}] {c['name']}")

    backends = [cv2.CAP_DSHOW, cv2.CAP_MSMF] if os.name == 'nt' else [cv2.CAP_ANY]

    # Prioridade Máxima: Insistir na Câmera USB configurada (Índice 1) com até 3 tentativas
    for attempt in range(1, 4):
        for backend in backends:
            backend_name = "DSHOW" if backend == cv2.CAP_DSHOW else ("MSMF" if backend == cv2.CAP_MSMF else "ANY")
            log(f"  [Tentativa {attempt}/3] Conectando a Webcam USB (Indice {preferred_index}) [{backend_name}]...")
            cap, w, h, brightness, status = test_camera_device(preferred_index, backend, backend_name)

            if cap is not None:
                log(f"  [SUCESSO] Webcam USB ATIVA no Indice {preferred_index} ({backend_name})! Resolucao: {w}x{h}, Brilho medio: {brightness:.1f}")
                log("=" * 60)
                _ACTIVE_CAMERA_INDEX = preferred_index
                _ACTIVE_BACKEND = backend_name
                return cap, preferred_index
            else:
                log(f"  Indice {preferred_index} [{backend_name}]: {status}")
        time.sleep(0.6)

    # Fallback apenas se a USB não responder de forma alguma
    log(f"  [AVISO] Nao foi possivel abrir a Webcam USB no Indice {preferred_index}. Verificando outros indices...")
    candidates = [i for i in [0, 2, 3] if i != preferred_index]

    for idx in candidates:
        for backend in backends:
            backend_name = "DSHOW" if backend == cv2.CAP_DSHOW else ("MSMF" if backend == cv2.CAP_MSMF else "ANY")
            log(f"  Testando Indice alternativo {idx} [{backend_name}]...")
            cap, w, h, brightness, status = test_camera_device(idx, backend, backend_name)
            if cap is not None:
                log(f"  [FALLBACK] Camera alternativa conectada no Indice {idx} ({backend_name})! Resolucao: {w}x{h}")
                log("=" * 60)
                _ACTIVE_CAMERA_INDEX = idx
                _ACTIVE_BACKEND = backend_name
                return cap, idx

    log("ERRO CRITICO: Nenhuma camera retornou imagem valida.")
    log("=" * 60)
    _ACTIVE_CAMERA_INDEX = None
    _ACTIVE_BACKEND = "N/A"
    return None, None

def main():
    global _CAMERA_ACTIVE, _LATEST_JPEG, _LAST_MOTION_TIME, _IS_CAMERA_UNLOCKED, _LAST_CAPTURE_TIME
    os.makedirs(INPUT_DIR, exist_ok=True)
    log("Iniciando modulo de captura da camera (Mosaico EXPO)...")

    # Inicia servidor HTTP de streaming
    http_thread = threading.Thread(target=start_stream_server, args=(STREAM_PORT,), daemon=True)
    http_thread.start()

    preferred_index = 1  # Padrao: 1 para a Camera desejada
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                config_data = json.load(f)
                preferred_index = config_data.get("cameraIndex", 1)
        except Exception as e:
            log(f"Aviso ao ler camera_config.json: {e}")

    cap, camera_index = open_best_camera(preferred_index)
    if not cap:
        log("Nova tentativa em 5 segundos...")
        time.sleep(5)
        cap, camera_index = open_best_camera(preferred_index)

    _CAMERA_ACTIVE = (cap is not None and cap.isOpened())

    ref_gray = None
    last_ref_time = time.time()
    start_time = time.time()
    last_heartbeat_time = time.time()
    consecutive_black_frames = 0
    consecutive_read_failures = 0

    try:
        while True:
            now = time.time()

            # Reconexão se câmera perdida
            if not cap or not cap.isOpened():
                _CAMERA_ACTIVE = False
                if now - last_heartbeat_time > 8.0:
                    log("Aguardando câmera ser conectada...")
                    last_heartbeat_time = now
                    cap, camera_index = open_best_camera(preferred_index)
                    _CAMERA_ACTIVE = (cap is not None and cap.isOpened())
                time.sleep(1.0)
                continue

            _CAMERA_ACTIVE = True

            try:
                ret, frame = cap.read()
            except Exception as e:
                log(f"Exceção ao ler frame: {e}. Reiniciando câmera...")
                try: cap.release()
                except Exception: pass
                time.sleep(1.0)
                cap, camera_index = open_best_camera(preferred_index)
                _CAMERA_ACTIVE = (cap is not None and cap.isOpened())
                continue

            if not ret or frame is None or frame.size == 0:
                consecutive_read_failures += 1
                if consecutive_read_failures >= 25:
                    log("Aviso: Múltiplas falhas de leitura. Reiniciando câmera...")
                    try: cap.release()
                    except Exception: pass
                    cap, camera_index = open_best_camera(preferred_index)
                    consecutive_read_failures = 0
                time.sleep(0.1)
                continue

            consecutive_read_failures = 0

            # Validação contínua de imagem preta
            frame_mean = frame.mean()
            if frame_mean < 1.5:
                consecutive_black_frames += 1
                if consecutive_black_frames == 60:
                    log("ALERTA: A câmera está retornando frames 100% pretos há 3 segundos! Verifique lente/privacidade.")
                if consecutive_black_frames >= 120:
                    log("Tentando alternar/reiniciar câmera devido a tela preta contínua...")
                    try: cap.release()
                    except Exception: pass
                    cap, camera_index = open_best_camera(preferred_index)
                    consecutive_black_frames = 0
                    continue
            else:
                consecutive_black_frames = 0

            # Atualizar streaming MJPEG com enquadramento quadrado (mesma altura, largura quadrada)
            try:
                h, w = frame.shape[:2]
                side = min(h, w)
                sx = (w - side) // 2
                sy = (h - side) // 2
                square_stream = frame[sy:sy + side, sx:sx + side]

                if side > 720:
                    preview_frame = cv2.resize(square_stream, (720, 720), interpolation=cv2.INTER_AREA)
                else:
                    preview_frame = square_stream

                ret_enc, jpeg_data = cv2.imencode('.jpg', preview_frame, [int(cv2.IMWRITE_JPEG_QUALITY), 82])
                if ret_enc:
                    with _FRAME_LOCK:
                        _LATEST_JPEG = jpeg_data.tobytes()
            except Exception:
                pass

            # Verificação de status e travas
            unlocked, lock_reason = is_camera_unlocked()
            _IS_CAMERA_UNLOCKED = unlocked

            # Detecção de Presença / Movimento estável
            # Converte para tons de cinza com blur
            small_for_motion = cv2.resize(frame, (320, 240), interpolation=cv2.INTER_AREA)
            gray = cv2.cvtColor(small_for_motion, cv2.COLOR_BGR2GRAY)
            gray = cv2.GaussianBlur(gray, (21, 21), 0)

            if ref_gray is None:
                ref_gray = gray
                last_ref_time = now
                time.sleep(0.04)
                continue

            # Compara frame atual com frame de referência de ~0.4s atrás
            frame_delta = cv2.absdiff(ref_gray, gray)
            thresh = cv2.threshold(frame_delta, 25, 255, cv2.THRESH_BINARY)[1]
            thresh = cv2.dilate(thresh, None, iterations=2)

            non_zero = cv2.countNonZero(thresh)
            total_pixels = 320 * 240
            motion_ratio = non_zero / total_pixels

            # Atualiza frame de referência a cada 0.4 segundos
            if now - last_ref_time >= 0.4:
                ref_gray = gray
                last_ref_time = now

            motion_detected = (motion_ratio >= 0.015)  # 1.5% de pixels em movimento

            if motion_detected:
                _LAST_MOTION_TIME = now

            # Heartbeat periódico a cada 15s
            if now - last_heartbeat_time > 15.0:
                idle_sec = now - _LAST_MOTION_TIME
                status_str = "LIVRE para disparo" if unlocked else f"BLOQUEADA ({lock_reason})"
                log(f"Câmera [Índice {camera_index}]: {status_str}. Inatividade: {idle_sec:.1f}s. Brilho: {frame_mean:.1f}.")
                last_heartbeat_time = now

            # Se bloqueada pelo mural ou fila, não dispara
            if not unlocked:
                time.sleep(0.08)
                continue

            # Checa se passou o cooldown de captura e aquecimento inicial
            time_since_last_capture = now - _LAST_CAPTURE_TIME
            if motion_detected and (now - start_time > 3.0) and (time_since_last_capture > CAPTURE_COOLDOWN_SEC):
                log(f"Presença detectada (ratio: {motion_ratio:.3f})! Capturando foto...")

                # Limpa frames antigos do buffer
                for _ in range(4):
                    cap.grab()
                ret_flush, frame_flush = cap.read()
                if ret_flush and frame_flush is not None:
                    frame = frame_flush

                # Enquadramento quadrado (mesma altura, largura centralizada)
                fh, fw = frame.shape[:2]
                side = min(fh, fw)
                sx = (fw - side) // 2
                sy = (fh - side) // 2
                square_frame = frame[sy:sy + side, sx:sx + side]

                # Garante no mínimo 1280x1280 para máxima nitidez no mosaico
                if side < 1280:
                    frame_save = cv2.resize(square_frame, (1280, 1280), interpolation=cv2.INTER_CUBIC)
                else:
                    frame_save = square_frame

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"captura_foto_{timestamp}.jpg"
                filepath = os.path.join(INPUT_DIR, filename)

                cv2.imwrite(filepath, frame_save, [int(cv2.IMWRITE_JPEG_QUALITY), 100])
                log(f"Foto salva com sucesso em: {filepath} ({frame_save.shape[1]}x{frame_save.shape[0]})")

                _LAST_CAPTURE_TIME = time.time()
                _LAST_MOTION_TIME = time.time()
                ref_gray = None
                time.sleep(1.5)  # Aguarda watcher pegar o arquivo

            time.sleep(0.04)  # ~25 FPS loop

    except KeyboardInterrupt:
        log("Encerrando captura da câmera...")
    finally:
        _CAMERA_ACTIVE = False
        if cap:
            cap.release()
        log("Câmera liberada com sucesso.")

if __name__ == "__main__":
    main()
