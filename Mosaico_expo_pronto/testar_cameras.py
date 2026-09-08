#!/usr/bin/env python3
"""
Testador Rápido de Câmeras - Mosaico EXPO
Testa as câmeras nos índices 0 e 1, salvando uma foto de cada:
- teste_camera_0.jpg
- teste_camera_1.jpg
Assim você pode abrir as fotos e ver com 100% de certeza qual é a webcam USB e qual é a câmera onboard do notebook.
"""

import os
import sys
import time
import cv2

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

print("=" * 65)
print("TESTADOR DE CAMERAS - MOSAICO EXPO")
print("=" * 65)

indices_to_test = [0, 1]

for idx in indices_to_test:
    print(f"\n[TESTE] Conectando a camera no Indice {idx}...")
    backend = cv2.CAP_DSHOW if os.name == 'nt' else cv2.CAP_ANY
    cap = cv2.VideoCapture(idx, backend)
    
    if not cap.isOpened():
        print(f"  -> Indice {idx}: Nao foi possivel abrir (camera ausente ou ocupada).")
        continue

    if os.name == 'nt':
        cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*'MJPG'))
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)

    # Aquecimento do sensor
    for _ in range(10):
        cap.read()
        time.sleep(0.06)

    ret, frame = cap.read()
    cap.release()

    if ret and frame is not None and frame.size > 0:
        output_file = os.path.join(ROOT_DIR, f"teste_camera_{idx}.jpg")
        cv2.imwrite(output_file, frame)
        h, w = frame.shape[:2]
        mean_b = frame.mean()
        print(f"  [OK] Foto salva com sucesso: teste_camera_{idx}.jpg")
        print(f"       Resolucao: {w}x{h} | Brilho medio: {mean_b:.1f}")
        print(f"       -> Abra 'teste_camera_{idx}.jpg' para conferir!")
    else:
        print(f"  [ERRO] Indice {idx} abriu, mas nao retornou frame valido.")

print("\n" + "=" * 65)
print("FIM DO TESTE DE CAMERAS")
print("Abra 'teste_camera_0.jpg' e 'teste_camera_1.jpg' na pasta para ver qual e a USB!")
print("=" * 65)
