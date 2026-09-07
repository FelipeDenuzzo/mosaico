import sys
import os
import time
import json

print("=" * 60)
print("DIAGNOSTICO DO SISTEMA MOSAICO EXPO")
print("=" * 60)

# 1. Python version
print(f"1. Versao do Python: {sys.version.split()[0]}")

# 2. Bibliotecas
print("\n2. Verificando Bibliotecas:")
for pkg in ["cv2", "PIL", "pillow_heif", "pillow_avif", "watchdog"]:
    try:
        __import__(pkg)
        print(f"   [OK] {pkg}")
    except ImportError:
        print(f"   [FALHA] {pkg} NAO esta instalado!")

# 3. Camera
print("\n3. Verificando Todas as Cameras Conectadas (USB e Nativas):")
try:
    import cv2
    cameras_encontradas = []
    
    # Testa indices de 0 a 5
    for idx in range(6):
        found_for_index = False
        backends = [cv2.CAP_DSHOW, cv2.CAP_MSMF] if os.name == 'nt' else [cv2.CAP_ANY]
        
        for backend in backends:
            backend_nome = "DirectShow" if backend == cv2.CAP_DSHOW else "MediaFoundation"
            try:
                cap = cv2.VideoCapture(idx, backend)
                if cap.isOpened():
                    # Le ate 5 frames de aquecimento (necessario para certas webcams USB)
                    for _ in range(5):
                        ret, frame = cap.read()
                        if ret and frame is not None and frame.size > 0:
                            h, w = frame.shape[:2]
                            print(f"   [OK] Camera encontrada no INDICE {idx} ({backend_nome}): {w}x{h}")
                            cameras_encontradas.append((idx, backend_nome, w, h))
                            found_for_index = True
                            break
                        time.sleep(0.08)
                    cap.release()
                if found_for_index:
                    break
            except Exception:
                pass

    if cameras_encontradas:
        print(f"\n   Total de cameras ativas detectadas: {len(cameras_encontradas)}")
        print("   Dica: No arquivo camera_config.json, altere 'cameraIndex' para o indice desejado:")
        for (c_idx, b_name, w, h) in cameras_encontradas:
            tipo = "Provavel Webcam USB Externa" if c_idx != 0 else "Provavel Camera Nativa / Principal"
            print(f"   -> cameraIndex = {c_idx} ({tipo} - {w}x{h})")
    else:
        print("   [ATENCAO] Nenhuma camera retornou imagem!")
        print("   Possiveis causas:")
        print("   1. O cabo da webcam USB nao esta bem conectado.")
        print("   2. Outro aplicativo (ex: app Camera do Windows, Teams, Edge) esta com a webcam aberta.")
        print("   3. As permissoes de camera estao desativadas em 'Configuracoes do Windows -> Privacidade -> Camera'.")
except Exception as e:
    print(f"   [ERRO] Falha ao testar camera: {e}")

# 4. Banco de tiles
print("\n4. Verificando Banco de Dados de Tiles:")
try:
    import sqlite3
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, "tiles_index.db")
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM tiles")
        total = cur.fetchone()[0]
        conn.close()
        print(f"   [OK] tiles_index.db encontrado com {total} tiles indexados.")
    else:
        print("   [ERRO] tiles_index.db nao encontrado!")
except Exception as e:
    print(f"   [ERRO] Falha ao ler banco: {e}")

print("\n" + "=" * 60)
print("Fim do teste de diagnostico.")
print("=" * 60)
