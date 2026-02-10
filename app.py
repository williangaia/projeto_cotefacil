# app.py - VIEW
import customtkinter as ctk
from tkinter import filedialog, messagebox
from pathlib import Path
import threading

class TelaInicial(ctk.CTkToplevel):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.parent = parent
        
        self.title("Selecionar Layout")
        self.geometry("300x200")
        
        # Centralizar na tela
        self.transient(parent)
        self.grab_set()
        
        ctk.CTkLabel(self, text="Selecione o tipo de layout:").pack(pady=20)
        
        self.bnt_consinco = ctk.CTkButton(
            self, 
            text="Layout Consinco", 
            command=lambda: self.selecionar_layout("consinco")
        )
        self.bnt_consinco.pack(pady=10)
        
        self.bnt_cotefacil = ctk.CTkButton(
            self, 
            text="Layout Cotefácil", 
            command=lambda: self.selecionar_layout("cotefacil")
        )
        self.bnt_cotefacil.pack(pady=10)
        
        self.layout_selecionado = None
    
    def selecionar_layout(self, layout):
        self.layout_selecionado = layout
        self.destroy()
        self.parent.abrir_tela_processamento(layout)

class App(ctk.CTk):
    def __init__(self, controller):
        super().__init__()
        self.controller = controller
        self.layout_atual = None
        
        self.title("Cotefácil")
        self.geometry("400x300")
        
        # Botão inicial
        self.btn_iniciar = ctk.CTkButton(
            self, 
            text="Iniciar Processamento", 
            command=self.abrir_tela_inicial
        )
        self.btn_iniciar.pack(pady=50)
        
        self.tela_processamento = None
    
    def abrir_tela_inicial(self):
        TelaInicial(self, self.controller)
    
    def abrir_tela_processamento(self, layout):
        self.layout_atual = layout
        
        # Destruir tela anterior se existir
        if self.tela_processamento:
            self.tela_processamento.destroy()
        
        # Criar nova tela baseada no layout
        if layout == "consinco":
            self.tela_processamento = TelaConsinco(self, self.controller)
        else:  # cotefacil
            self.tela_processamento = TelaCotefacil(self, self.controller)

class TelaBase(ctk.CTkFrame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.parent = parent
        
    def _configurar_processamento(self, numero_cotacao, pasta_saida):
        self.bnt_processar.configure(state="disabled")
        
        threading.Thread(
            target=self._executar_processamento,
            args=(numero_cotacao, pasta_saida)
        ).start()
    
    def _executar_processamento(self, numero_cotacao, pasta_saida):
        try:
            # Método abstrato, implementado nas subclasses
            pass
        except Exception as e:
            import traceback
            erro = traceback.format_exc()
            print(erro)
            self.after(0, lambda: messagebox.showerror("Erro ao processar", str(e)))
        finally:
            self.after(0, lambda: self.bnt_processar.configure(state="normal"))

class TelaConsinco(TelaBase):
    def __init__(self, parent, controller):
        super().__init__(parent, controller)
        self.pack(fill="both", expand=True, padx=20, pady=20)
        
        self.caminho_txt = None
        
        self.entry_cotacao = ctk.CTkEntry(
            self, 
            placeholder_text="Digite o número da cotação:"
        )
        self.entry_cotacao.pack(pady=10)
        
        self.bnt_txt = ctk.CTkButton(
            self, 
            text="Selecione o pedido (.txt)", 
            command=self.selecionar_txt
        )
        self.bnt_txt.pack(pady=10)
        
        self.bnt_processar = ctk.CTkButton(
            self, 
            text="Processar", 
            command=self.processar
        )
        self.bnt_processar.pack(pady=10)
    
    def selecionar_txt(self):
        arquivo = filedialog.askopenfilename(filetypes=[("TXT", "*.txt")])
        if arquivo:
            self.caminho_txt = Path(arquivo)
            messagebox.showinfo("Arquivo selecionado", f"Arquivo: {self.caminho_txt.name}")
    
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
        
        self._configurar_processamento(numero, pasta)
    
    def _executar_processamento(self, numero_cotacao, pasta_saida):
        try:
            self.controller.processar_cotacao(
                numero_cotacao,
                "consinco",
                self.caminho_txt,
                pasta_saida
            )
            self.after(0, lambda: messagebox.showinfo("Sucesso", "Cotação processada (Layout Consinco)"))
        except Exception as e:
            super()._executar_processamento(numero_cotacao, pasta_saida)

class TelaCotefacil(TelaBase):
    def __init__(self, parent, controller):
        super().__init__(parent, controller)
        self.pack(fill="both", expand=True, padx=20, pady=20)
        
        self.entry_cotacao = ctk.CTkEntry(
            self, 
            placeholder_text="Digite o número da cotação:"
        )
        self.entry_cotacao.pack(pady=20)
        
        self.bnt_processar = ctk.CTkButton(
            self, 
            text="Gerar CSV Cotefácil", 
            command=self.processar
        )
        self.bnt_processar.pack(pady=10)
    
    def processar(self):
        try:
            numero = int(self.entry_cotacao.get())
        except ValueError:
            messagebox.showerror("Erro", "Número de cotação inválido")
            return
        
        pasta = Path(filedialog.askdirectory(title="Selecione a pasta de saída"))
        if not pasta:
            return
        
        self._configurar_processamento(numero, pasta)
    
    def _executar_processamento(self, numero_cotacao, pasta_saida):
        try:
            self.controller.processar_cotacao(
                numero_cotacao,
                "cotefacil",
                pasta_saida=pasta_saida
            )
            self.after(0, lambda: messagebox.showinfo("Sucesso", "CSV Cotefácil gerado com sucesso"))
        except Exception as e:
            super()._executar_processamento(numero_cotacao, pasta_saida)