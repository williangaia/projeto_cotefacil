import customtkinter as ctk
from tkinter import filedialog, messagebox
from pathlib import Path
import threading

class App(ctk.CTk):

    def __init__(self, controller):
        super().__init__()
        self.controller = controller

        self.title("Cotefácil")
        self.geometry("400x250")

        self.entry_cotacao = ctk.CTkEntry(self, placeholder_text="Digite o número da cotação: ")
        self.entry_cotacao.pack(pady=10)

        self.bnt_txt = ctk.CTkButton(self, text="Selecione o pedido (.txt)", command=self.selecionar_txt)
        self.bnt_txt.pack(pady=10)

        self.bnt_processar = ctk.CTkButton(self, text="Processar", command=self.processar)
        self.bnt_processar.pack(pady=10)

        self.caminho_txt = None

    def selecionar_txt(self):
        arquivo = filedialog.askopenfilename(filetypes=[("TXT", "*.txt")])
        if arquivo:
            self.caminho_txt = Path(arquivo)

    def processar(self):
        if not self.caminho_txt:
            messagebox.showerror("Erro", "Selecione o arquivo TXT")
            return

        try:
            numero = int(self.entry_cotacao.get())
        except ValueError:
            messagebox.showerror("Erro", "Número de cotação inválido")
            return
        
        pasta = Path(filedialog.askdirectory(title="Selecione a pasta de saída"))
        if not pasta:
            return
        
        pasta_saida = Path(pasta)

        self.bnt_processar.configure(state="disabled")

        threading.Thread(
            target=self._executar_processamento,
            args=(numero, pasta_saida),
            daemon=True
        ).start()

    def _executar_processamento(self, numero, pasta_saida):
        try:
            self.controller.processar_cotacao(
                numero,
                self.caminho_txt,
                pasta_saida
            )
            self.bnt_processar.configure(state="normal")
            self.after(0, lambda: messagebox.showinfo("Sucesso", "Cotação processada"))
        except Exception as e:
            import traceback
            erro = traceback.format_exc()
            print(erro)
            self.after(0, lambda: messagebox.showerror("Erro ao processar", erro))