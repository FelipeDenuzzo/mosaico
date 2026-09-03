import os
import shutil
import json

# Definição de pastas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.join(BASE_DIR, "Mosaico_expo")
DEST_DIR = os.path.join(BASE_DIR, "Mosaico_expo_pronto")

# Pastas e arquivos a serem copiados
DIRECTORIES_TO_COPY = ["acervo", "camera"]
FILES_TO_COPY = [
    "auto_mosaic.py",
    "indexador.py",
    "mosaico.py",
    "serve_site.py",
    "watch-manifest.js",
    "requirements.txt",
    "package.json",
    "mosaico-exibicao.html",
    "start_mosaico_expo.bat",
    "stop_mosaico_expo.bat",
    "tiles_index.db",
    "camera_config.json",
    "fix_manifest.js",
    "pixi.min.js"
]

# Pastas vazias a serem criadas
EMPTY_DIRECTORIES = ["input", "Output", "processing", "archive", "error"]

def main():
    print("==================================================")
    print("Iniciando empacotamento do Mosaico para o Pendrive...")
    print(f"Origem: {SOURCE_DIR}")
    print(f"Destino: {DEST_DIR}")
    print("==================================================")

    # 1. Limpar destino caso já exista
    if os.path.exists(DEST_DIR):
        print(f"- Removendo pasta de destino anterior: {DEST_DIR}")
        shutil.rmtree(DEST_DIR)
    
    os.makedirs(DEST_DIR)

    # 2. Copiar Diretórios
    for folder in DIRECTORIES_TO_COPY:
        src_folder = os.path.join(SOURCE_DIR, folder)
        dest_folder = os.path.join(DEST_DIR, folder)
        if os.path.exists(src_folder):
            print(f"- Copiando pasta: {folder}...")
            # Copiar ignorando __pycache__ e arquivos .DS_Store
            shutil.copytree(
                src_folder, 
                dest_folder, 
                ignore=shutil.ignore_patterns("__pycache__", ".DS_Store", "*.pyc")
            )
        else:
            print(f"[AVISO] Pasta não encontrada: {folder}")

    # 3. Copiar Arquivos
    for filename in FILES_TO_COPY:
        src_file = os.path.join(SOURCE_DIR, filename)
        dest_file = os.path.join(DEST_DIR, filename)
        if os.path.exists(src_file):
            print(f"- Copiando arquivo: {filename}...")
            shutil.copy2(src_file, dest_file)
        else:
            print(f"[AVISO] Arquivo não encontrado: {filename}")

    # 4. Criar Pastas Vazias de Estado/Processamento
    for folder in EMPTY_DIRECTORIES:
        dest_folder = os.path.join(DEST_DIR, folder)
        print(f"- Criando pasta vazia: {folder}/")
        os.makedirs(dest_folder, exist_ok=True)

    # 5. Inicializar arquivos de estado limpos
    print("- Inicializando arquivos de estado (manifest.json, jobs.json, job_counter.txt)...")
    
    # manifest.json
    clean_manifest = {
        "mosaics": [],
        "queue": [],
        "seen": [],
        "isBusy": False,
        "isBusyTimestamp": 0
    }
    with open(os.path.join(DEST_DIR, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(clean_manifest, f, indent=2, ensure_ascii=False)

    # jobs.json
    with open(os.path.join(DEST_DIR, "jobs.json"), "w", encoding="utf-8") as f:
        json.dump({}, f, indent=2, ensure_ascii=False)

    # job_counter.txt
    with open(os.path.join(DEST_DIR, "job_counter.txt"), "w", encoding="utf-8") as f:
        f.write("0\n")

    print("==================================================")
    print("✓ Mosaico empacotado com sucesso!")
    print(f"A pasta '{os.path.basename(DEST_DIR)}' está pronta para ser copiada para o pendrive.")
    print("==================================================")

if __name__ == "__main__":
    main()
