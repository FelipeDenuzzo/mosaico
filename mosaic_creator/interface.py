"""
Interface gráfica para criação de mosaicos de imagens.
"""

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import threading
import os
import time
import signal
import platform
import queue
from PIL import Image
from mosaico import (
    criar_mosaico,
    calcular_tamanho_final,
    FIXED_COLUMNS,
    FIXED_TILE_SIZE,
    FIXED_COLOR_VARIATION,
    FIXED_MAX_REPETITIONS,
    MIN_BASE_WIDTH,
)


class InterfaceMosaico:
    """Interface gráfica para criar mosaicos."""
    
    def __init__(self, janela):
        self.janela = janela
        self.janela.title("Criador de Mosaicos")
        self.janela.geometry("700x950")
        self.janela.resizable(False, True)
        
        self.imagem_base = None
        self.categoria_selecionada = "Informacao"
        self.processando = False
        self.fila_ui = queue.Queue()

        # Container rolável da interface principal
        self.canvas = tk.Canvas(self.janela, highlightthickness=0)
        self.scrollbar = ttk.Scrollbar(self.janela, orient="vertical", command=self.canvas.yview)
        self.frame_principal = tk.Frame(self.canvas)
        self._canvas_window = self.canvas.create_window((0, 0), window=self.frame_principal, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        self.frame_principal.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )
        self.canvas.bind(
            "<Configure>",
            lambda e: self.canvas.itemconfigure(self._canvas_window, width=e.width)
        )
        self.frame_principal.bind("<Enter>", self._bind_mousewheel)
        self.frame_principal.bind("<Leave>", self._unbind_mousewheel)
        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")
        
        # Impedir que o App entre em sleep
        signal.signal(signal.SIGCONT, self._manter_vivo)
        
        # Thread para manter o processo vivo
        self.thread_keep_alive = threading.Thread(target=self._keep_alive_loop, daemon=True)
        self.thread_keep_alive.start()
        
        # Atualizar janela continuamente
        self._atualizar_janela()
        self.janela.after(100, self._processar_fila_ui)
        
        self._criar_interface()
    
    def _manter_vivo(self, signum, frame):
        """Signal handler para manter vivo"""
        pass
    
    def _keep_alive_loop(self):
        """Loop que mantém o processo ativo em background"""
        while True:
            try:
                time.sleep(0.5)
                # Simula atividade
                _ = os.getpid()
            except:
                pass
    
    def _atualizar_janela(self):
        """Atualiza a janela continuamente para evitar freeze"""
        try:
            self.janela.update_idletasks()
        except:
            pass
        # Agendar próxima atualização
        self.janela.after(100, self._atualizar_janela)

    def _processar_fila_ui(self):
        try:
            while True:
                tipo, dados = self.fila_ui.get_nowait()

                if tipo == "progresso":
                    atual, total = dados
                    porcentagem = (atual / total) * 100 if total else 0
                    self.label_progresso.config(text=f"Progresso: {atual}/{total}", fg="blue")
                    self.barra_progresso.config(value=porcentagem)

                elif tipo == "status":
                    texto, cor = dados
                    self.label_progresso.config(text=texto, fg=cor)

                elif tipo == "sucesso":
                    caminho_saida, largura, altura = dados
                    self.label_progresso.config(text="✓ Concluído!", fg="green")
                    self.barra_progresso.config(value=100)
                    self.processando = False
                    self.botao_rodar.config(state="normal")
                    messagebox.showinfo(
                        "Sucesso",
                        f"Mosaico criado com sucesso!\n\n"
                        f"Arquivo: {caminho_saida}\n"
                        f"Tamanho: {largura}×{altura} pixels"
                    )

                elif tipo == "erro":
                    erro = dados
                    self.label_progresso.config(text="✗ Erro!", fg="red")
                    self.processando = False
                    self.botao_rodar.config(state="normal")
                    messagebox.showerror("Erro", f"Erro ao criar mosaico:\n{erro}")

                elif tipo == "finalizar":
                    self.processando = False
                    self.botao_rodar.config(state="normal")

        except queue.Empty:
            pass

        self.janela.after(100, self._processar_fila_ui)

    def _on_mousewheel(self, event):
        sistema = platform.system()

        if sistema == "Windows":
            self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        elif sistema == "Darwin":
            self.canvas.yview_scroll(int(-1 * event.delta), "units")
        else:  # Linux
            if event.num == 4:
                self.canvas.yview_scroll(-1, "units")
            elif event.num == 5:
                self.canvas.yview_scroll(1, "units")

    def _bind_mousewheel(self, event=None):
        sistema = platform.system()
        if sistema == "Linux":
            self.canvas.bind_all("<Button-4>", self._on_mousewheel)
            self.canvas.bind_all("<Button-5>", self._on_mousewheel)
        else:
            self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)

    def _unbind_mousewheel(self, event=None):
        sistema = platform.system()
        if sistema == "Linux":
            self.canvas.unbind_all("<Button-4>")
            self.canvas.unbind_all("<Button-5>")
        else:
            self.canvas.unbind_all("<MouseWheel>")
    
    def _criar_interface(self):
        """Cria todos os elementos da interface."""

        # ===== CATEGORIA DE PIXELS =====
        frame_cat = tk.LabelFrame(self.frame_principal, text="1. Categoria de Pixels", padx=10, pady=10)
        frame_cat.pack(fill="x", padx=10, pady=5)

        self.var_categoria = tk.StringVar(value="Informacao")
        categorias = ["Informacao", "Medicamentos", "Pornografia", "Geral"]

        for nome in categorias:
            tk.Radiobutton(frame_cat, text=nome, variable=self.var_categoria, value=nome, command=self._atualizar_categoria).pack(anchor="w")

        # ===== IMAGEM BASE =====
        frame1 = tk.LabelFrame(self.frame_principal, text="2. Imagem Base", padx=10, pady=10)
        frame1.pack(fill="x", padx=10, pady=5)

        self.label_base = tk.Label(frame1, text="Nenhuma imagem selecionada", fg="gray")
        self.label_base.pack(anchor="w")
        tk.Button(frame1, text="Selecionar Imagem Base", command=self._selecionar_base).pack(fill="x", pady=5)

        # ===== PASTA PIXELS (agora automática) =====
        frame2 = tk.LabelFrame(self.frame_principal, text="3. Base de Pixels", padx=10, pady=10)
        frame2.pack(fill="x", padx=10, pady=5)

        # Pasta definida automaticamente pela categoria selecionada.

        # Inicializa pasta de pixels
        self._atualizar_categoria()

        # ===== CONFIGURAÇÃO FIXA =====
        frame3 = tk.LabelFrame(self.frame_principal, text="4. Configuração Fixa", padx=10, pady=10)
        frame3.pack(fill="x", padx=10, pady=5)
        tk.Label(
            frame3,
            text=(
                f"Configuração fixa: {FIXED_COLUMNS} colunas, tiles {FIXED_TILE_SIZE}x{FIXED_TILE_SIZE} px, "
                f"variação de cor {FIXED_COLOR_VARIATION}, repetição máxima {FIXED_MAX_REPETITIONS}."
            ),
            fg="gray",
            justify="left",
            wraplength=640,
        ).pack(anchor="w")
        
        # ===== TAMANHO FINAL E NOME =====
        frame6 = tk.LabelFrame(self.frame_principal, text="5. Visualização e Nome do Arquivo", padx=10, pady=10)
        frame6.pack(fill="x", padx=10, pady=5)
        
        self.label_tamanho = tk.Label(frame6, text="Tamanho final: selecione a imagem base", fg="gray")
        self.label_tamanho.pack(anchor="w", pady=5)
        
        tk.Label(frame6, text="Nome do arquivo (sem extensão):").pack(anchor="w", pady=(10, 0))
        self.entry_nome = tk.Entry(frame6)
        self.entry_nome.insert(0, "mosaico")
        self.entry_nome.pack(fill="x", pady=5)
        
        # ===== ONDE SALVAR =====
        frame7 = tk.LabelFrame(self.frame_principal, text="6. Onde Salvar", padx=10, pady=10)
        frame7.pack(fill="x", padx=10, pady=5)
        
        self.label_saida = tk.Label(frame7, text="Pasta: Documentos", fg="gray")
        self.label_saida.pack(anchor="w")
        tk.Button(frame7, text="Escolher Pasta de Saída", command=self._selecionar_saida).pack(fill="x", pady=5)
        
        self.pasta_saida = os.path.expanduser("~/Documentos")
        
        # ===== BOTÃO RODAR E PROGRESSO =====
        frame8 = tk.Frame(self.frame_principal)
        frame8.pack(fill="x", padx=10, pady=10, side="bottom")
        
        self.botao_rodar = tk.Button(frame8, text="RODAR", command=self._rodar_mosaico, bg="#4CAF50", fg="white", font=("Arial", 14, "bold"), height=2)
        self.botao_rodar.pack(fill="x", pady=5)
        
        self.label_progresso = tk.Label(frame8, text="", fg="blue")
        self.label_progresso.pack(anchor="w")
        
        self.barra_progresso = ttk.Progressbar(frame8, mode="determinate", length=400)
        self.barra_progresso.pack(fill="x", pady=5)

    def _atualizar_categoria(self):
        """Atualiza a categoria selecionada."""
        self.categoria_selecionada = self.var_categoria.get()
    
    def _selecionar_base(self):
        """Abre diálogo para selecionar imagem base."""
        caminho = filedialog.askopenfilename(
            title="Selecione a imagem base",
            filetypes=[("Imagens", "*.jpg *.jpeg *.png *.bmp"), ("Todas", "*.*")]
        )
        if caminho:
            self.imagem_base = caminho
            nome = os.path.basename(caminho)
            self.label_base.config(text=f"✓ {nome}", fg="black")
            self._atualizar_nome_arquivo()
            self._calcular_tamanho()
    
    def _selecionar_saida(self):
        """Abre diálogo para selecionar pasta de saída."""
        pasta = filedialog.askdirectory(title="Onde salvar o arquivo?")
        if pasta:
            self.pasta_saida = pasta
            nome = os.path.basename(pasta) or pasta
            self.label_saida.config(text=f"Pasta: {nome}", fg="black")
    
    def _atualizar_nome_arquivo(self):
        """Atualiza o nome do arquivo com base nas configurações."""
        if self.imagem_base:
            nome_base = os.path.splitext(os.path.basename(self.imagem_base))[0]
            nome_sugerido = f"{nome_base}_{FIXED_COLUMNS}x{FIXED_TILE_SIZE}"
            self.entry_nome.delete(0, tk.END)
            self.entry_nome.insert(0, nome_sugerido)
    
    def _calcular_tamanho(self):
        """Calcula e exibe o tamanho final."""
        if not self.imagem_base or not self.var_categoria.get():
            messagebox.showwarning("Aviso", "Selecione a imagem base e a categoria de pixels!")
            return
        
        try:
            self.label_tamanho.config(text=f"Tamanho final: {FIXED_COLUMNS} x {FIXED_COLUMNS} tiles ({FIXED_COLUMNS * FIXED_TILE_SIZE}x{FIXED_COLUMNS * FIXED_TILE_SIZE} px)", fg="black")
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao calcular tamanho: {e}")
    
    def _rodar_mosaico(self):
        """Inicia a criação do mosaico em thread separada."""
        if not self.imagem_base or not self.var_categoria.get():
            messagebox.showwarning("Aviso", "Selecione a categoria de pixels e a imagem base!")
            return
        
        if self.processando:
            messagebox.showwarning("Aviso", "Já há um processamento em andamento!")
            return
        
        self.botao_rodar.config(state="disabled")
        self.processando = True
        thread = threading.Thread(target=self._thread_criar_mosaico, daemon=False)
        thread.start()
    
    def _thread_criar_mosaico(self):
        """Thread que cria o mosaico."""
        try:
            with Image.open(self.imagem_base) as img_base:
                if img_base.width < MIN_BASE_WIDTH:
                    raise ValueError("A imagem base deve ter no mínimo 1000 px de largura.")

            nome_arquivo = self.entry_nome.get().strip() or "mosaico"
            
            # Garantir que o nome não tenha extensão
            if nome_arquivo.endswith(".jpg"):
                nome_arquivo = nome_arquivo[:-4]
            
            caminho_saida = os.path.join(self.pasta_saida, f"{nome_arquivo}.jpg")
            
            # Callback para progresso
            def atualizar_progresso(atual, total):
                self.fila_ui.put(("progresso", (atual, total)))
            
            self.fila_ui.put(("status", ("Iniciando...", "blue")))
            
            largura, altura = criar_mosaico(
                self.imagem_base,
                self.var_categoria.get(),
                FIXED_TILE_SIZE,
                True,
                FIXED_MAX_REPETITIONS,
                FIXED_COLOR_VARIATION,
                caminho_saida,
                callback_progresso=atualizar_progresso
            )
            
            self.fila_ui.put(("sucesso", (caminho_saida, largura, altura)))
        except Exception as e:
            self.fila_ui.put(("erro", str(e)))
        finally:
            pass


if __name__ == "__main__":
    janela = tk.Tk()
    app = InterfaceMosaico(janela)
    janela.mainloop()
