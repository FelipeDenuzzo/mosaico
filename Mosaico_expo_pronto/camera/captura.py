import os
import time
import json
from datetime import datetime
import cv2

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(BASE_DIR, "input")
CONFIG_PATH = os.path.join(BASE_DIR, "camera_config.json")
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "camera_log.txt")

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def is_camera_unlocked() -> bool:
    # 1. Verificar se há arquivos na pasta input (evita race condition durante estabilização)
    if os.path.exists(INPUT_DIR):
        try:
            files = [f for f in os.listdir(INPUT_DIR) if not f.startswith(".")]
            if len(files) > 0:
                return False
        except Exception:
            pass

    # 2. Verificar manifest.json
    manifest_path = os.path.join(BASE_DIR, "manifest.json")
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, "r") as f:
                data = json.load(f)
                is_busy = data.get("isBusy", False)
                is_busy_timestamp = data.get("isBusyTimestamp", 0)
                queue = data.get("queue", [])
                
                # Watchdog: se isBusy estiver true por mais de 45 segundos, ignoramos a trava
                if is_busy and is_busy_timestamp > 0:
                    age_ms = time.time() * 1000 - is_busy_timestamp
                    if age_ms > 45000:
                        is_busy = False # Ignora a trava (evita deadlocks)
                        
                if is_busy or len(queue) > 0:
                    return False
        except Exception:
            pass

    # 3. Verificar jobs.json
    jobs_path = os.path.join(BASE_DIR, "jobs.json")
    if os.path.exists(jobs_path):
        try:
            with open(jobs_path, "r") as f:
                jobs = json.load(f)
                for job_id, job in jobs.items():
                    if job.get("status") == "processando":
                        return False
        except Exception:
            pass

    return True

def open_best_camera(preferred_index=0):
    candidates = [preferred_index]
    for i in [0, 1, 2, 3]:
        if i not in candidates:
            candidates.append(i)

    log(f"Iniciando deteccao de camera funcional (preferencia: indice {preferred_index})...")
    
    backends = [cv2.CAP_DSHOW, cv2.CAP_MSMF] if os.name == 'nt' else [cv2.CAP_ANY]
    for idx in candidates:
        for backend in backends:
            backend_name = "DSHOW" if backend == cv2.CAP_DSHOW else ("MSMF" if backend == cv2.CAP_MSMF else "DEFAULT")
            try:
                cap = cv2.VideoCapture(idx, backend)
                if cap.isOpened():
                    # Tenta ler ate 5 frames para garantir que o sensor inicializou
                    for _ in range(5):
                        ret, test_frame = cap.read()
                        if ret and test_frame is not None and test_frame.size > 0:
                            # Tenta definir resolucao alta
                            cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1920)
                            cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 1080)
                            log(f"✓ Camera FUNCIONANDO no indice {idx} (backend: {backend_name}, resolucao: {test_frame.shape[1]}x{test_frame.shape[0]})!")
                            
                            # Atualiza config com o indice verificado
                            try:
                                cfg = {}
                                if os.path.exists(CONFIG_PATH):
                                    with open(CONFIG_PATH, "r") as f:
                                        cfg = json.load(f)
                                cfg["cameraIndex"] = idx
                                with open(CONFIG_PATH, "w") as f:
                                    json.dump(cfg, f, indent=2)
                            except Exception:
                                pass
                            return cap, idx
                        time.sleep(0.1)
                    cap.release()
            except Exception as e:
                pass
                
    log("ERRO: Nenhuma camera retornou frames de video validos. Verifique se a webcam esta conectada e com permissoes ativas.")
    return None, None

def main():
    os.makedirs(INPUT_DIR, exist_ok=True)
    log("Iniciando modulo de captura da camera...")

    # Carrega configuracoes (padrao: 1 para webcam USB externa)
    preferred_index = 1
    interval_seconds = 60
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r") as f:
                config_data = json.load(f)
                preferred_index = config_data.get("cameraIndex", 1)
                interval_seconds = config_data.get("intervalSeconds", 60)
        except Exception as e:
            log(f"Aviso ao ler camera_config.json: {e}")

    cap, camera_index = open_best_camera(preferred_index)
    if not cap:
        log("Tentando novamente em 5 segundos...")
        time.sleep(5)
        cap, camera_index = open_best_camera(preferred_index)
        if not cap:
            log("Falha critica: Nenhuma webcam disponivel. O script continuara tentando a cada 10s...")

    prev_gray = None
    motion_threshold = 0.015  # 1.5% de pixels alterados
    start_time = time.time()
    last_photo_time = time.time()
    last_heartbeat_time = time.time()
    consecutive_read_failures = 0

    try:
        while True:
            now = time.time()

            # Se camera nao estiver aberta, tenta reconectar
            if not cap or not cap.isOpened():
                if now - last_heartbeat_time > 10.0:
                    log("Aguardando conexao com a camera...")
                    last_heartbeat_time = now
                    cap, camera_index = open_best_camera(preferred_index)
                time.sleep(1.0)
                continue

            ret, frame = cap.read()
            if not ret or frame is None:
                consecutive_read_failures += 1
                if consecutive_read_failures >= 30: # 3 segundos sem frames
                    log("Aviso: Falhas consecutivas de leitura da camera. Tentando reiniciar...")
                    cap.release()
                    cap, camera_index = open_best_camera(camera_index)
                    consecutive_read_failures = 0
                time.sleep(0.1)
                continue

            consecutive_read_failures = 0
            unlocked = is_camera_unlocked()

            # Heartbeat informativo a cada 15 segundos
            if now - last_heartbeat_time > 15.0:
                status_str = "LIVRE para disparo" if unlocked else "BLOQUEADA (mural/fila ocupada)"
                log(f"Camera ativa [indice {camera_index}]. Status: {status_str}.")
                last_heartbeat_time = now

            # Se a tela ou back-end estiverem ocupados, bloqueia novas fotos
            if not unlocked:
                prev_gray = None
                time.sleep(0.2)
                continue

            # Converter para tons de cinza e aplicar desfoque Gaussiano
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            gray = cv2.GaussianBlur(gray, (21, 21), 0)

            if prev_gray is None:
                prev_gray = gray
                time.sleep(0.1)
                continue

            # Diferença absoluta entre frames consecutivos
            frame_delta = cv2.absdiff(prev_gray, gray)
            thresh = cv2.threshold(frame_delta, 25, 255, cv2.THRESH_BINARY)[1]
            thresh = cv2.dilate(thresh, None, iterations=2)

            non_zero = cv2.countNonZero(thresh)
            total_pixels = frame.shape[0] * frame.shape[1]
            motion_ratio = non_zero / total_pixels

            prev_gray = gray
            motion_detected = motion_ratio > motion_threshold

            # Gatilho de tempo (intervalSeconds como fallback para teste/exposicao)
            time_since_photo = now - last_photo_time
            interval_trigger = (interval_seconds > 0) and (time_since_photo >= interval_seconds)

            # Disparar se houver movimento (ou intervalo) e após estabilização inicial de 3s
            if (motion_detected or interval_trigger) and (now - start_time > 3.0):
                motivo = f"Movimento detectado (ratio: {motion_ratio:.3f})" if motion_detected else f"Intervalo de {interval_seconds}s atingido"
                log(f"{motivo}! Capturando foto...")

                # Limpa frames antigos acumulados no buffer da webcam
                for _ in range(5):
                    cap.grab()
                ret_flush, frame_flush = cap.read()
                if ret_flush and frame_flush is not None:
                    frame = frame_flush

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"captura_foto_{timestamp}.jpg"
                filepath = os.path.join(INPUT_DIR, filename)

                # Salva a imagem no formato JPEG com qualidade máxima (100)
                cv2.imwrite(filepath, frame, [int(cv2.IMWRITE_JPEG_QUALITY), 100])
                log(f"Foto salva com sucesso em: {filepath}")

                last_photo_time = time.time()
                prev_gray = None

                # Aguarda o watcher iniciar o processamento antes de checar a trava de novo
                time.sleep(1.5)

            time.sleep(0.1)

    except KeyboardInterrupt:
        log("Encerrando script de captura...")
    finally:
        if cap:
            cap.release()
        log("Webcam liberada com sucesso.")

if __name__ == "__main__":
    main()
