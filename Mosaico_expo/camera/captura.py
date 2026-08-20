import os
import time
import json
from datetime import datetime
import cv2

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(BASE_DIR, "input")
CONFIG_PATH = os.path.join(BASE_DIR, "camera_config.json")

def get_interval():
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r") as f:
                data = json.load(f)
                return max(5, int(data.get("intervalSeconds", 60)))
        except Exception:
            pass
    return 60

def main():
    os.makedirs(INPUT_DIR, exist_ok=True)
    print("Iniciando controle da webcam...")
    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        print("Erro: Câmera não encontrada.")
        return

    # Tenta definir resolução HD para a imagem de entrada
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1920)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 1080)

    print("Câmera ativa. Entrando em loop de captura com sensor de movimento.")
    
    last_capture_time = 0
    prev_gray = None
    motion_threshold = 0.015  # 1.5% de pixels alterados
    start_time = time.time()
    
    try:
        while True:
            interval = get_interval()
            now = time.time()
            
            ret, frame = cap.read()
            if not ret:
                time.sleep(0.1)
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
            
            # Disparar se houver movimento, pós-estabilização inicial de 3s e após o cooldown/intervalo
            if motion_detected and (now - start_time > 3.0) and (now - last_capture_time >= interval):
                # Limpa frames antigos acumulados no buffer da webcam
                for _ in range(5):
                    cap.grab()
                ret_flush, frame_flush = cap.read()
                if ret_flush:
                    frame = frame_flush
                    
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"captura_foto_{timestamp}.jpg"
                filepath = os.path.join(INPUT_DIR, filename)
                
                # Salva a imagem no formato JPEG com qualidade alta (95)
                cv2.imwrite(filepath, frame, [int(cv2.IMWRITE_JPEG_QUALITY), 95])
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Movimento detectado (ratio: {motion_ratio:.3f})! Foto salva em: {filepath} (cooldown: {interval}s)")
                last_capture_time = now
            
            # Controle de taxa de quadros (aprox. 10 FPS)
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        print("Encerrando script de captura...")
    finally:
        cap.release()
        print("Webcam liberada com sucesso.")

if __name__ == "__main__":
    main()
