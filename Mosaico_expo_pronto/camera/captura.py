import os
import time
import json
from datetime import datetime
import cv2

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(BASE_DIR, "input")
CONFIG_PATH = os.path.join(BASE_DIR, "camera_config.json")

def is_camera_unlocked():
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
                        is_busy = False # Ignora a trava (evita deadlocks se o navegador suspender)
                        
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

def main():
    os.makedirs(INPUT_DIR, exist_ok=True)
    print("Iniciando controle da webcam...")

    # Carrega índice da câmera a partir do camera_config.json (padrão: 1 para câmera externa, fallback para 0)
    camera_index = 1
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r") as f:
                config_data = json.load(f)
                camera_index = config_data.get("cameraIndex", 1)
        except Exception as e:
            print(f"Aviso ao ler camera_config.json: {e}")

    print(f"Tentando abrir câmera no índice {camera_index}...")
    if os.name == 'nt':
        cap = cv2.VideoCapture(camera_index, cv2.CAP_DSHOW)
        if not cap.isOpened():
            cap = cv2.VideoCapture(camera_index)
    else:
        cap = cv2.VideoCapture(camera_index)

    if not cap.isOpened() and camera_index != 0:
        print(f"Aviso: Câmera no índice {camera_index} não respondeu. Tentando índice 0 (câmera padrão)...")
        camera_index = 0
        if os.name == 'nt':
            cap = cv2.VideoCapture(0, cv2.CAP_DSHOW)
            if not cap.isOpened():
                cap = cv2.VideoCapture(0)
        else:
            cap = cv2.VideoCapture(0)

    if not cap.isOpened():
        print("Erro: Nenhuma câmera encontrada.")
        return

    # Tenta definir resolução HD para a imagem de entrada
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1920)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 1080)

    print("Câmera ativa. Entrando em loop de captura com sensor de movimento.")
    
    prev_gray = None
    motion_threshold = 0.015  # 1.5% de pixels alterados
    start_time = time.time()
    
    try:
        while True:
            now = time.time()
            
            ret, frame = cap.read()
            if not ret:
                time.sleep(0.1)
                continue
                
            # Se a tela ou back-end estiverem ocupados, bloqueia novas fotos
            if not is_camera_unlocked():
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
            
            # Contar quantidade de pixels brancos (alterados)
            non_zero = cv2.countNonZero(thresh)
            total_pixels = frame.shape[0] * frame.shape[1]
            motion_ratio = non_zero / total_pixels
            
            # Atualizar frame anterior
            prev_gray = gray
            
            # Detecção de movimento com threshold
            motion_detected = motion_ratio > motion_threshold
            
            # Disparar se houver movimento e pós-estabilização inicial de 3s
            if motion_detected and (now - start_time > 3.0):
                # Limpa frames antigos acumulados no buffer da webcam
                for _ in range(5):
                    cap.grab()
                ret_flush, frame_flush = cap.read()
                if ret_flush:
                    frame = frame_flush
                    
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"captura_foto_{timestamp}.jpg"
                filepath = os.path.join(INPUT_DIR, filename)
                
                # Salva a imagem no formato JPEG com qualidade máxima (100)
                cv2.imwrite(filepath, frame, [int(cv2.IMWRITE_JPEG_QUALITY), 100])
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Movimento detectado (ratio: {motion_ratio:.3f})! Foto salva em: {filepath}")
                
                # Aguarda o watcher iniciar o processamento antes de checar a trava de novo
                time.sleep(1.0)
            
            # Controle de taxa de quadros (aprox. 10 FPS)
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        print("Encerrando script de captura...")
    finally:
        cap.release()
        print("Webcam liberada com sucesso.")

if __name__ == "__main__":
    main()
