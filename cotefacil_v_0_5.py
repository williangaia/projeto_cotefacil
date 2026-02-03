import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinterdnd2 import DND_FILES, TkinterDnD
from typing import List, Tuple, Dict, Set
import os
from datetime import datetime
import threading
import time 
import snorte  # Sua biblioteca personalizada para conexão Oracle
"""

### Última versão com separação por fornecedores ####

"""
# Configuração do diretório de rede para salvar os arquivos
DIRETORIO_REDE = r"\\10.106.31.86\d$\NeoGridClient\documents\in"

# Classe para processar os arquivos Cotefácil com melhor performance
class ProcessadorArquivoCotefacil:
    def __init__(self):
        self.dados_coletados = []
        self.cnpj_comprador_atual = None
        self.cnpj_fornecedor_atual = None
        self.codigo_pedido_atual = None
        self.codigo_barras_atual = None
        self.quantidade_atual = None
        # Novo: dicionário para agrupar por fornecedor
        self.dados_por_fornecedor = {}
        
    def ler_arquivo_txt(self, caminho_arquivo: str) -> List[str]:
        """Lê arquivo de forma mais eficiente"""
        try:
            with open(caminho_arquivo, 'r', encoding='utf-8') as arquivo:
                # Lê todas as linhas de uma vez (mais rápido para arquivos não muito grandes)
                linhas = arquivo.read().splitlines()
            return [linha.strip() for linha in linhas if linha.strip()]
        except Exception as e:
            raise Exception(f"Erro ao ler arquivo: {e}")
    
    def processar_linha(self, linha: str) -> bool:
        """Processa linha individual com validações otimizadas"""
        if not linha or ';' not in linha:
            return False
            
        campos = linha.split(';')
        tipo_registro = campos[0]
        
        try:
            if tipo_registro == '1' and len(campos) >= 2:
                self.cnpj_comprador_atual = campos[1]
                return False
                
            elif tipo_registro == '2' and len(campos) >= 5:
                self.cnpj_fornecedor_atual = campos[1]
                self.codigo_pedido_atual = campos[4]
                return False
                
            elif tipo_registro == '3' and len(campos) >= 4:
                if all([self.cnpj_comprador_atual, self.cnpj_fornecedor_atual, self.codigo_pedido_atual]):
                    self.codigo_barras_atual = campos[1]
                    self.quantidade_atual = campos[3]
                    return True
            
            return False
                
        except Exception:
            return False
    
    def adicionar_registro_atual(self):
        """Adiciona registro atual à lista de dados coletados, agrupando por fornecedor"""
        if all([self.codigo_barras_atual, self.cnpj_fornecedor_atual, 
                self.cnpj_comprador_atual, self.quantidade_atual, self.codigo_pedido_atual]):
            
            registro = f"{self.codigo_barras_atual};{self.cnpj_fornecedor_atual};{self.cnpj_comprador_atual};{self.quantidade_atual};{self.codigo_pedido_atual}"
            self.dados_coletados.append(registro)
            
            # Agrupar por fornecedor
            if self.cnpj_fornecedor_atual not in self.dados_por_fornecedor:
                self.dados_por_fornecedor[self.cnpj_fornecedor_atual] = []
            self.dados_por_fornecedor[self.cnpj_fornecedor_atual].append(registro)
    
    def processar_arquivo_completo(self, caminho_arquivo: str) -> Dict[str, List[str]]:
        """Processa arquivo completo com reset de estado e retorna dados agrupados por fornecedor"""
        # Reset do estado para cada arquivo
        self.dados_coletados = []
        self.cnpj_comprador_atual = None
        self.cnpj_fornecedor_atual = None
        self.codigo_pedido_atual = None
        self.codigo_barras_atual = None
        self.quantidade_atual = None
        self.dados_por_fornecedor = {}
        
        linhas = self.ler_arquivo_txt(caminho_arquivo)
        
        if not linhas:
            return {}
        
        for linha in linhas:
            produto_processado = self.processar_linha(linha)
            if produto_processado:
                self.adicionar_registro_atual()
        
        return self.dados_por_fornecedor

# Sistema de Cache Avançado
class CacheConsulta:
    def __init__(self):
        self.cache_produtos: Dict[str, str] = {}
        self.cache_fornecedores: Dict[str, str] = {}
        self.cache_empresas: Dict[str, str] = {}
        self.nao_encontrados: Set[str] = set()  # Para evitar consultas repetidas de dados não encontrados
        
    def limpar_cache(self):
        """Limpa todo o cache"""
        self.cache_produtos.clear()
        self.cache_fornecedores.clear()
        self.cache_empresas.clear()
        self.nao_encontrados.clear()
        
    def get_tamanho_cache(self) -> Dict[str, int]:
        """Retorna estatísticas do cache"""
        return {
            'produtos': len(self.cache_produtos),
            'fornecedores': len(self.cache_fornecedores),
            'empresas': len(self.cache_empresas),
            'nao_encontrados': len(self.nao_encontrados)
        }

# Classe para consultas no banco com cache
class ConsultasBanco:
    def __init__(self, connection, cache: CacheConsulta):
        self.connection = connection
        self.cache = cache
        
    def consultar_produto_por_codigo_barras(self, codigo_barras: str) -> List[Tuple]:
        """Consulta SEQPRODUTO no banco usando código de barras com cache"""
        # Verifica cache primeiro
        if codigo_barras in self.cache.cache_produtos:
            return [(codigo_barras, self.cache.cache_produtos[codigo_barras])]
            
        if codigo_barras in self.cache.nao_encontrados:
            return []
            
        try:
            query = """
            SELECT 
                A.CODACESSO,
                A.SEQPRODUTO
            FROM MAP_PRODCODIGO A
            WHERE A.CODACESSO = :codigo_barras
            """
            result = self.connection.cursor.execute(query, codigo_barras=codigo_barras)
            resultados = result.fetchall()
            
            # Atualiza cache
            if resultados:
                self.cache.cache_produtos[codigo_barras] = str(resultados[0][1])
            else:
                self.cache.nao_encontrados.add(codigo_barras)
                
            return resultados
        except Exception as e:
            print(f"Erro na consulta de produto: {e}")
            self.cache.nao_encontrados.add(codigo_barras)
            return []
    
    def consultar_fornecedor_por_cnpj(self, cnpj: str) -> List[Tuple]:
        """Consulta SEQFORNECEDOR no banco usando CNPJ com cache"""
        if cnpj in self.cache.cache_fornecedores:
            return [(cnpj[:12], cnpj[12:], self.cache.cache_fornecedores[cnpj])]
            
        if cnpj in self.cache.nao_encontrados:
            return []
            
        try:
            if len(cnpj) == 14:
                nrocgccpf = cnpj[:12]
                digcgccpf = cnpj[12:]
            else:
                nrocgccpf = cnpj
                digcgccpf = "00"
                
            query = """
            SELECT
                P.NROCGCCPF,
                P.DIGCGCCPF,
                P.SEQPESSOA
            FROM GE_PESSOA P
            WHERE P.NROCGCCPF = :nrocgccpf 
            AND P.DIGCGCCPF = :digcgccpf
            """
            result = self.connection.cursor.execute(query, nrocgccpf=nrocgccpf, digcgccpf=digcgccpf)
            resultados = result.fetchall()
            
            if resultados:
                self.cache.cache_fornecedores[cnpj] = str(resultados[0][2])
            else:
                self.cache.nao_encontrados.add(cnpj)
                
            return resultados
        except Exception as e:
            print(f"Erro na consulta de fornecedor: {e}")
            self.cache.nao_encontrados.add(cnpj)
            return []
    
    def consultar_empresa_por_cnpj(self, cnpj: str) -> List[Tuple]:
        """Consulta NROEMPRESA no banco usando CNPJ com cache"""
        if cnpj in self.cache.cache_empresas:
            return [(cnpj[:12], cnpj[12:], self.cache.cache_empresas[cnpj])]
            
        if cnpj in self.cache.nao_encontrados:
            return []
            
        try:
            if len(cnpj) == 14:
                nrocgc = cnpj[:12]
                digcgc = cnpj[12:]
            else:
                nrocgc = cnpj
                digcgc = "00"
              
            query = """
            SELECT
                A.NROCGC,
                A.DIGCGC,
                A.NROEMPRESA
            FROM MAX_EMPRESA A
            WHERE A.NROCGC = :nrocgc 
            AND A.DIGCGC = :digcgc
            """
            result = self.connection.cursor.execute(query, nrocgc=nrocgc, digcgc=digcgc)
            resultados = result.fetchall()
            
            if resultados:
                self.cache.cache_empresas[cnpj] = str(resultados[0][2])
            else:
                self.cache.nao_encontrados.add(cnpj)
                
            return resultados
        except Exception as e:
            print(f"Erro na consulta de empresa: {e}")
            self.cache.nao_encontrados.add(cnpj)
            return []

# Classe para processar dados com consultas ao banco otimizadas
class ProcessadorComConsultas:
    def __init__(self, connection, cache: CacheConsulta):
        self.connection = connection
        self.cache = cache
        self.consultas = ConsultasBanco(connection, cache)
        
    def processar_e_cruzar_dados(self, dados_por_fornecedor: Dict[str, List[str]]) -> Dict[str, List[Tuple]]:
        """Processa os dados e faz os cruzamentos com o banco de forma otimizada, mantendo separação por fornecedor"""
        dados_finais_por_fornecedor = {}
        fornecedores_nao_encontrados = []
        
        for cnpj_fornecedor, registros in dados_por_fornecedor.items():
            dados_finais_fornecedor = []
            registros_invalidos = 0
            
            # Pré-processamento: extrair dados únicos para consultas em lote
            codigos_barras_unicos = set()
            cnpjs_empresas_unicos = set()
            
            for registro in registros:
                try:
                    campos = registro.split(';')
                    if len(campos) != 5:
                        registros_invalidos += 1
                        continue
                        
                    codigo_barras, cnpj_fornecedor_reg, cnpj_empresa, quantidade, pedido = campos
                    
                    if codigo_barras and codigo_barras not in self.cache.cache_produtos and codigo_barras not in self.cache.nao_encontrados:
                        codigos_barras_unicos.add(codigo_barras)
                    if cnpj_empresa and cnpj_empresa not in self.cache.cache_empresas and cnpj_empresa not in self.cache.nao_encontrados:
                        cnpjs_empresas_unicos.add(cnpj_empresa)
                        
                except Exception:
                    registros_invalidos += 1
            
            # Consultar fornecedor atual (apenas uma vez por fornecedor)
            resultados_fornecedor = self.consultas.consultar_fornecedor_por_cnpj(cnpj_fornecedor)
            seqfornecedor_final = self.cache.cache_fornecedores.get(cnpj_fornecedor, "")
            
            if not seqfornecedor_final:
                fornecedores_nao_encontrados.append(cnpj_fornecedor)
                continue  # Pular este fornecedor se não encontrado
            
            # Consultas em lote para produtos e empresas
            for codigo_barras in codigos_barras_unicos:
                self.consultas.consultar_produto_por_codigo_barras(codigo_barras)
                
            for cnpj in cnpjs_empresas_unicos:
                self.consultas.consultar_empresa_por_cnpj(cnpj)
            
            # Processamento final para este fornecedor
            for registro in registros:
                try:
                    campos = registro.split(';')
                    if len(campos) != 5:
                        continue
                        
                    codigo_barras, _, cnpj_empresa, quantidade, pedido = campos
                    
                    # Cruzamentos usando cache
                    seqproduto_final = self.cache.cache_produtos.get(codigo_barras, "")
                    seqpessoaemp_final = self.cache.cache_empresas.get(cnpj_empresa, "")
                    
                    # Só adiciona se todos os cruzamentos foram bem sucedidos
                    if all([seqproduto_final, seqfornecedor_final, seqpessoaemp_final]):
                        dados_finais_fornecedor.append((
                            seqproduto_final,
                            seqfornecedor_final,
                            seqpessoaemp_final,
                            quantidade,
                            pedido
                        ))
                    
                except Exception as e:
                    registros_invalidos += 1
            
            if dados_finais_fornecedor:
                dados_finais_por_fornecedor[cnpj_fornecedor] = dados_finais_fornecedor
        
        return dados_finais_por_fornecedor, fornecedores_nao_encontrados

# Interface principal com processamento assíncrono
class InterfaceProcessador:
    def __init__(self):
        self.janela = TkinterDnD.Tk()
        self.janela.title("Processador Cotefácil - Por Fornecedor")
        self.janela.geometry("800x600")
        
        self.processador = ProcessadorArquivoCotefacil()
        self.connection = None
        self.arquivo_selecionado = None
        self.cache = CacheConsulta()
        self.processando = False
        
        # Novas variáveis para controle de salvamento por fornecedor
        self.dados_cruzados_por_fornecedor = {}
        self.fornecedores_processados = []
        self.fornecedores_nao_encontrados = []
        self.nome_arquivo_original = ""
        
        self.criar_interface()
        
    def criar_interface(self):
        # Título
        titulo = tk.Label(self.janela, text="Processador de Arquivos Cotefácil - Um Pedido por Vez", 
                         font=("Arial", 14, "bold"))
        titulo.pack(pady=15)
        
        # Frame para seleção de arquivos
        frame_arquivo = tk.LabelFrame(self.janela, text="Seleção de Arquivo", font=("Arial", 10))
        frame_arquivo.pack(padx=20, pady=10, fill="x")
        
        # Botões de arquivo
        frame_botoes = tk.Frame(frame_arquivo)
        frame_botoes.pack(pady=5)
        
        btn_selecionar = tk.Button(frame_botoes, text="📂 Selecionar Arquivo", 
                                  command=self.selecionar_arquivo, font=("Arial", 9))
        btn_selecionar.pack(side="left", padx=5)
        
        btn_limpar = tk.Button(frame_botoes, text="🗑️ Limpar Seleção", 
                              command=self.limpar_selecao, font=("Arial", 9))
        btn_limpar.pack(side="left", padx=5)
        
        # Área de "arrastar e soltar"
        self.label_arquivo = tk.Label(frame_arquivo, 
                                     text="Arraste um arquivo aqui ou clique em 'Selecionar Arquivo'",
                                     bg="lightgray", relief="sunken", height=3,
                                     font=("Arial", 9), cursor="hand2")
        self.label_arquivo.pack(padx=10, pady=10, fill="x")
        self.label_arquivo.bind('<Button-1>', self.selecionar_arquivo)
        
        # Informações do arquivo selecionado
        self.label_info_arquivo = tk.Label(frame_arquivo, 
                                         text="Nenhum arquivo selecionado",
                                         font=("Arial", 9), fg="gray")
        self.label_info_arquivo.pack(padx=10, pady=5, fill="x")
        
        # Configurar drag and drop para arquivos
        self.janela.drop_target_register(DND_FILES)
        self.janela.dnd_bind('<<Drop>>', self.arquivo_arrastado)
        
        # Frame de controle
        frame_controle = tk.Frame(self.janela)
        frame_controle.pack(pady=10)
        
        # Botão processar
        self.btn_processar = tk.Button(frame_controle, text="🚀 Processar Arquivo", 
                                      command=self.iniciar_processamento_assincrono, 
                                      state="disabled", font=("Arial", 11), height=2,
                                      bg="#4CAF50", fg="white")
        self.btn_processar.pack(side="left", padx=5)
        
        # Botão salvar fornecedores (inicialmente desabilitado)
        self.btn_salvar_fornecedores = tk.Button(frame_controle, text="💾 Salvar Fornecedores", 
                                                command=self.mostrar_dialogo_salvamento,
                                                state="disabled", font=("Arial", 11), height=2,
                                                bg="#2196F3", fg="white")
        self.btn_salvar_fornecedores.pack(side="left", padx=5)
        
        # Botão limpar cache
        self.btn_limpar_cache = tk.Button(frame_controle, text="🔄 Limpar Cache", 
                                         command=self.limpar_cache, 
                                         font=("Arial", 9))
        self.btn_limpar_cache.pack(side="left", padx=5)
        
        # Label de status do cache
        self.label_cache = tk.Label(frame_controle, text="Cache: 0/0/0", 
                                   font=("Arial", 8), fg="gray")
        self.label_cache.pack(side="left", padx=10)
        
        # Label de diretório de saída
        self.label_diretorio = tk.Label(frame_controle, 
                                       text=f"Saída: {DIRETORIO_REDE}",
                                       font=("Arial", 8), fg="blue")
        self.label_diretorio.pack(side="left", padx=10)
        
        # Área de log
        frame_log = tk.LabelFrame(self.janela, text="Log de Processamento", font=("Arial", 10))
        frame_log.pack(padx=20, pady=10, fill="both", expand=True)
        
        self.texto_log = tk.Text(frame_log, height=12, font=("Consolas", 8))
        scrollbar = tk.Scrollbar(frame_log, command=self.texto_log.yview)
        self.texto_log.config(yscrollcommand=scrollbar.set)
        
        self.texto_log.pack(side="left", fill="both", expand=True, padx=5, pady=5)
        scrollbar.pack(side="right", fill="y", pady=5)
        
        # Barra de progresso
        self.barra_progresso = ttk.Progressbar(self.janela, mode='determinate')
        self.barra_progresso.pack(padx=20, pady=5, fill="x")
        
        # Label de status
        self.label_status = tk.Label(self.janela, text="Pronto", font=("Arial", 8))
        self.label_status.pack(pady=2)
        
        # Configurar fechamento da janela
        self.janela.protocol("WM_DELETE_WINDOW", self.fechar_aplicacao)
        
        # Atualizar status do cache periodicamente
        self.atualizar_status_cache()
        
    def atualizar_status_cache(self):
        """Atualiza o status do cache na interface"""
        if hasattr(self, 'cache'):
            stats = self.cache.get_tamanho_cache()
            texto = f"Cache: P{stats['produtos']}/F{stats['fornecedores']}/E{stats['empresas']}"
            self.label_cache.config(text=texto)
        
        # Agenda próxima atualização
        self.janela.after(5000, self.atualizar_status_cache)
    
    def adicionar_log(self, mensagem: str):
        """Adiciona mensagem à área de log de forma thread-safe"""
        def atualizar_log():
            self.texto_log.insert(tk.END, f"{datetime.now().strftime('%H:%M:%S')} - {mensagem}\n")
            self.texto_log.see(tk.END)
            self.janela.update()
        
        self.janela.after(0, atualizar_log)
    
    def atualizar_status(self, mensagem: str):
        """Atualiza a barra de status de forma thread-safe"""
        def atualizar():
            self.label_status.config(text=mensagem)
        
        self.janela.after(0, atualizar)
    
    def atualizar_progresso(self, valor: int, maximo: int = 100):
        """Atualiza a barra de progresso de forma thread-safe"""
        def atualizar():
            if maximo > 0:
                self.barra_progresso['value'] = (valor / maximo) * 100
            else:
                self.barra_progresso['value'] = valor
        
        self.janela.after(0, atualizar)
    
    def limpar_cache(self):
        """Limpa o cache de consultas"""
        self.cache.limpar_cache()
        self.adicionar_log("🔄 Cache limpo")
        self.atualizar_status_cache()
    
    def limpar_selecao(self):
        """Limpa a seleção do arquivo"""
        self.arquivo_selecionado = None
        self.label_arquivo.config(bg="lightgray", 
                                text="Arraste um arquivo aqui ou clique em 'Selecionar Arquivo'")
        self.label_info_arquivo.config(text="Nenhum arquivo selecionado")
        self.btn_processar.config(state="disabled")
        self.btn_salvar_fornecedores.config(state="disabled")
        self.adicionar_log("📁 Seleção de arquivo limpa")
        
        # Limpar dados de processamento
        self.dados_cruzados_por_fornecedor = {}
        self.fornecedores_processados = []
        self.fornecedores_nao_encontrados = []
    
    def arquivo_arrastado(self, event):
        """Processa arquivo arrastado para a janela"""
        if self.processando:
            return
            
        try:
            caminhos_arquivos = self.janela.tk.splitlist(event.data)
            if caminhos_arquivos:
                caminho_arquivo = caminhos_arquivos[0].strip('{}')
                
                if os.path.isfile(caminho_arquivo) and caminho_arquivo.lower().endswith('.txt'):
                    self.carregar_arquivo(caminho_arquivo)
                else:
                    messagebox.showerror("Erro", "Por favor, selecione apenas um arquivo .txt!")
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao processar arquivo: {str(e)}")
    
    def selecionar_arquivo(self, event=None):
        """Abre diálogo para selecionar um único arquivo"""
        if self.processando:
            return
            
        arquivo = filedialog.askopenfilename(
            title="Selecione um arquivo Cotefácil",
            filetypes=[("Arquivos texto", "*.txt")]
        )
        if arquivo:
            self.carregar_arquivo(arquivo)
    
    def carregar_arquivo(self, caminho_arquivo: str):
        """Carrega arquivo selecionado"""
        self.arquivo_selecionado = caminho_arquivo
        nome_arquivo = os.path.basename(caminho_arquivo)
        tamanho_arquivo = os.path.getsize(caminho_arquivo) / 1024  # KB
        
        self.label_arquivo.config(bg="lightgreen", 
                                text=f"📁 Arquivo selecionado: {nome_arquivo}")
        self.label_info_arquivo.config(text=f"Arquivo: {nome_arquivo} | Tamanho: {tamanho_arquivo:.2f} KB")
        self.btn_processar.config(state="normal")
        self.btn_salvar_fornecedores.config(state="disabled")
        self.adicionar_log(f"✅ Arquivo carregado: {nome_arquivo}")
    
    def conectar_banco(self) -> bool:
        """Conecta ao banco de dados"""
        try:
            self.adicionar_log("🔗 Conectando ao banco de dados...")
            self.connection = snorte.Snorte()
            self.adicionar_log("✅ Conexão com o banco estabelecida")
            return True
        except Exception as e:
            self.adicionar_log(f"❌ Falha na conexão: {str(e)}")
            return False
    
    def iniciar_processamento_assincrono(self):
        """Inicia o processamento em thread separada"""
        if self.processando:
            return
            
        self.processando = True
        self.btn_processar.config(state="disabled", text="⏳ Processando...")
        self.btn_salvar_fornecedores.config(state="disabled")
        
        # Iniciar thread de processamento
        thread = threading.Thread(target=self.processar_arquivo_thread)
        thread.daemon = True
        thread.start()
    
    def processar_arquivo_thread(self):
        """Processa arquivo em thread separada"""
        try:
            self._processar_arquivo()
        except Exception as e:
            self.adicionar_log(f"❌ Erro na thread: {str(e)}")
        finally:
            # Restaurar interface
            self.janela.after(0, self._finalizar_processamento)
    
    def _processar_arquivo(self):
        """Processa o arquivo selecionado"""
        if not self.arquivo_selecionado:
            self.adicionar_log("❌ Nenhum arquivo selecionado!")
            return
        
        # Iniciar processamento
        self.atualizar_progresso(0)
        self.atualizar_status("Iniciando processamento...")
        
        try:
            self.texto_log.delete(1.0, tk.END)
            self.nome_arquivo_original = os.path.basename(self.arquivo_selecionado)
            self.adicionar_log("🔍 Iniciando processamento por fornecedor...")
            self.adicionar_log(f"📁 Arquivo: {self.nome_arquivo_original}")
            self.adicionar_log(f"📂 Diretório de saída: {DIRETORIO_REDE}")
            
            # Conectar ao banco
            if not self.conectar_banco():
                return
            
            # Processar arquivo
            self.atualizar_status("Processando arquivo...")
            self.atualizar_progresso(30)
            
            dados_por_fornecedor = self.processador.processar_arquivo_completo(self.arquivo_selecionado)
            
            if not dados_por_fornecedor:
                self.adicionar_log("❌ Nenhum dado válido encontrado no arquivo")
                return
            
            self.adicionar_log(f"✅ Encontrados {len(dados_por_fornecedor)} fornecedor(es) no arquivo")
            
            # Cruzar com banco
            self.adicionar_log("\n🎯 Cruzando dados com banco (usando cache)...")
            self.atualizar_status("Cruzando dados com banco...")
            self.atualizar_progresso(60)
            
            processador_consultas = ProcessadorComConsultas(self.connection, self.cache)
            self.dados_cruzados_por_fornecedor, self.fornecedores_nao_encontrados = processador_consultas.processar_e_cruzar_dados(dados_por_fornecedor)
            
            # Mostrar fornecedores não encontrados
            if self.fornecedores_nao_encontrados:
                self.adicionar_log(f"⚠️ Fornecedores não encontrados no banco: {len(self.fornecedores_nao_encontrados)}")
                for cnpj in self.fornecedores_nao_encontrados:
                    self.adicionar_log(f"   ❌ CNPJ: {cnpj}")
            
            total_fornecedores_processados = len(self.dados_cruzados_por_fornecedor)
            total_registros = sum(len(registros) for registros in self.dados_cruzados_por_fornecedor.values())
            
            self.adicionar_log(f"✅ {total_registros} registros cruzados com sucesso para {total_fornecedores_processados} fornecedor(es)")
            self.adicionar_log(f"📊 Estatísticas do cache: {self.cache.get_tamanho_cache()}")
            
            if not self.dados_cruzados_por_fornecedor:
                self.adicionar_log("❌ Nenhum registro pôde ser cruzado com o banco")
                return
            
            self.adicionar_log("\n📋 Fornecedores prontos para salvamento:")
            for i, (cnpj_fornecedor, registros) in enumerate(self.dados_cruzados_por_fornecedor.items(), 1):
                seqfornecedor = self.cache.cache_fornecedores.get(cnpj_fornecedor, "DESCONHECIDO")
                self.adicionar_log(f"   {i}. CNPJ: {cnpj_fornecedor} | SEQFORNECEDOR: {seqfornecedor} | Registros: {len(registros)}")
            
            self.adicionar_log(f"\n💾 Clique em 'Salvar Fornecedores' para escolher qual salvar primeiro")
            
            self.atualizar_status("Processamento concluído - Pronto para salvar")
            self.atualizar_progresso(100)
            
        except Exception as e:
            self.adicionar_log(f"❌ Erro durante o processamento: {str(e)}")
            self.atualizar_status(f"Erro: {str(e)}")
        
        finally:
            self.atualizar_progresso(100)
    
    def _finalizar_processamento(self):
        """Finaliza o processamento e restaura a interface"""
        self.processando = False
        self.btn_processar.config(state="normal", text="🚀 Processar Arquivo")
        
        # Habilitar botão de salvar fornecedores se houver dados processados
        if self.dados_cruzados_por_fornecedor:
            self.btn_salvar_fornecedores.config(state="normal")
        
        self.barra_progresso['value'] = 0
    
    def mostrar_dialogo_salvamento(self):
        """Mostra diálogo para escolher qual fornecedor salvar"""
        if not self.dados_cruzados_por_fornecedor:
            messagebox.showinfo("Nenhum Fornecedor", "Não há fornecedores para salvar.")
            return
        
        # Criar lista de fornecedores disponíveis (não salvos ainda)
        fornecedores_disponiveis = []
        for cnpj_fornecedor in self.dados_cruzados_por_fornecedor.keys():
            if cnpj_fornecedor not in self.fornecedores_processados:
                seqfornecedor = self.cache.cache_fornecedores.get(cnpj_fornecedor, "DESCONHECIDO")
                num_registros = len(self.dados_cruzados_por_fornecedor[cnpj_fornecedor])
                fornecedores_disponiveis.append((cnpj_fornecedor, seqfornecedor, num_registros))
        
        if not fornecedores_disponiveis:
            messagebox.showinfo("Todos Salvos", "Todos os fornecedores já foram salvos.")
            return
        
        # Criar diálogo de seleção
        dialogo = tk.Toplevel(self.janela)
        dialogo.title("Escolher Fornecedor para Salvar")
        dialogo.geometry("500x300")
        dialogo.transient(self.janela)
        dialogo.grab_set()
        
        # Título
        tk.Label(dialogo, text="Selecione um fornecedor para salvar:", 
                font=("Arial", 11, "bold")).pack(pady=10)
        
        # Lista de fornecedores
        frame_lista = tk.Frame(dialogo)
        frame_lista.pack(pady=10, padx=20, fill="both", expand=True)
        
        lista_fornecedores = tk.Listbox(frame_lista, font=("Arial", 10), height=8)
        scrollbar = tk.Scrollbar(frame_lista, orient="vertical")
        lista_fornecedores.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=lista_fornecedores.yview)
        
        # Adicionar fornecedores à lista
        self.indices_fornecedores = {}
        for i, (cnpj, seqfornecedor, num_registros) in enumerate(fornecedores_disponiveis):
            texto = f"Fornecedor {seqfornecedor} - CNPJ: {cnpj} - {num_registros} registro(s)"
            lista_fornecedores.insert(tk.END, texto)
            self.indices_fornecedores[i] = cnpj
        
        lista_fornecedores.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Informação adicional
        tk.Label(dialogo, text="Os fornecedores serão salvos um por vez na pasta de destino.", 
                font=("Arial", 9), fg="gray").pack(pady=5)
        
        # Controles
        frame_botoes = tk.Frame(dialogo)
        frame_botoes.pack(pady=15)
        
        btn_salvar = tk.Button(frame_botoes, text="💾 Salvar Selecionado", 
                              command=lambda: self.salvar_fornecedor_selecionado(dialogo, lista_fornecedores),
                              font=("Arial", 10), bg="#4CAF50", fg="white")
        btn_salvar.pack(side="left", padx=5)
        
        btn_cancelar = tk.Button(frame_botoes, text="Cancelar", 
                                command=dialogo.destroy,
                                font=("Arial", 10))
        btn_cancelar.pack(side="left", padx=5)
    
    def salvar_fornecedor_selecionado(self, dialogo, lista_fornecedores):
        """Salva o fornecedor selecionado pelo usuário"""
        selecao = lista_fornecedores.curselection()
        if not selecao:
            messagebox.showwarning("Nenhuma Seleção", "Por favor, selecione um fornecedor da lista.")
            return
        
        indice = selecao[0]
        if indice not in self.indices_fornecedores:
            messagebox.showerror("Erro", "Fornecedor selecionado inválido.")
            return
        
        cnpj_fornecedor = self.indices_fornecedores[indice]
        dialogo.destroy()
        
        # Salvar o fornecedor selecionado
        self.salvar_arquivo_fornecedor(cnpj_fornecedor)
    
    def salvar_arquivo_fornecedor(self, cnpj_fornecedor: str):
        """Gera um arquivo TXT para o fornecedor especificado"""
        if cnpj_fornecedor not in self.dados_cruzados_por_fornecedor:
            messagebox.showerror("Erro", f"Fornecedor {cnpj_fornecedor} não encontrado.")
            return
        
        try:
            # Verificar se o diretório de rede existe
            if not os.path.exists(DIRETORIO_REDE):
                self.adicionar_log(f"⚠️ Diretório não encontrado. Tentando criar: {DIRETORIO_REDE}")
                try:
                    os.makedirs(DIRETORIO_REDE, exist_ok=True)
                    self.adicionar_log(f"✅ Diretório criado com sucesso")
                except Exception as e:
                    self.adicionar_log(f"❌ Não foi possível criar o diretório: {str(e)}")
                    messagebox.showerror("Erro", f"Não foi possível acessar o diretório de rede:\n{DIRETORIO_REDE}\n\nErro: {str(e)}")
                    return
            
            # Obter dados do fornecedor
            registros = self.dados_cruzados_por_fornecedor[cnpj_fornecedor]
            seqfornecedor = self.cache.cache_fornecedores.get(cnpj_fornecedor, "DESCONHECIDO")
            
            # Gerar nome do arquivo com timestamp
            data_hora_atual = datetime.now().strftime('%Y%m%d_%H%M%S')
            nome_base = os.path.splitext(self.nome_arquivo_original)[0]
            data_processamento = datetime.now().strftime('%Y%m%d')
            
            # Nome do arquivo: [nome_base]_[seqfornecedor]_[timestamp].txt
            nome_arquivo = f"{nome_base}_F{seqfornecedor}_{data_hora_atual}.txt"
            caminho_completo = os.path.join(DIRETORIO_REDE, nome_arquivo)
            
            self.adicionar_log(f"\n💾 Salvando arquivo para fornecedor {cnpj_fornecedor}...")
            
            with open(caminho_completo, 'w', encoding='utf-8') as arquivo:
                for seqproduto, seqfornecedor, seqpessoaemp, sugestaolote, idcontroleinterno in registros:
                    # Formato: SEQPRODUTO;SEQFORNECEDOR;SEQPESSOAEMP;SUGESTAOLOTE;DATADEPROCESSAMENTO;1;1;DATADEPROCESSAMENTO;C;C(idcontroleinterno)
                    linha = f"{seqproduto};{seqfornecedor};{seqpessoaemp};{sugestaolote};{data_processamento};1;1;{data_processamento};C;N{idcontroleinterno}"
                    arquivo.write(linha + '\n')
            
            # Marcar fornecedor como processado
            self.fornecedores_processados.append(cnpj_fornecedor)
            
            self.adicionar_log(f"✅ Arquivo salvo: {nome_arquivo}")
            self.adicionar_log(f"📁 Caminho: {caminho_completo}")
            self.adicionar_log(f"📊 {len(registros)} registro(s) salvos com sucesso")
            
            # Verificar se ainda há fornecedores para salvar
            fornecedores_restantes = [f for f in self.dados_cruzados_por_fornecedor.keys() 
                                     if f not in self.fornecedores_processados]
            
            if fornecedores_restantes:
                resposta = messagebox.askyesno("Fornecedor Salvo", 
                                              f"Fornecedor {cnpj_fornecedor} salvo com sucesso!\n\n"
                                              f"Arquivo: {nome_arquivo}\n"
                                              f"Registros: {len(registros)}\n\n"
                                              f"Deseja salvar outro fornecedor agora?")
                
                if resposta:
                    self.mostrar_dialogo_salvamento()
                else:
                    self.adicionar_log(f"💡 Fornecedores restantes: {len(fornecedores_restantes)}")
            else:
                messagebox.showinfo("Processo Concluído", 
                                   f"Todos os fornecedores foram salvos com sucesso!\n\n"
                                   f"Total de fornecedores: {len(self.dados_cruzados_por_fornecedor)}\n"
                                   f"Total de registros: {sum(len(r) for r in self.dados_cruzados_por_fornecedor.values())}\n\n"
                                   f"Arquivos salvos em: {DIRETORIO_REDE}")
                self.adicionar_log(f"🎉 Todos os {len(self.dados_cruzados_por_fornecedor)} fornecedores foram salvos!")
                self.btn_salvar_fornecedores.config(state="disabled")
            
        except PermissionError as e:
            self.adicionar_log(f"❌ Erro de permissão ao salvar arquivo: {str(e)}")
            messagebox.showerror("Erro de Permissão", 
                               f"Não foi possível salvar o arquivo no diretório de rede.\n\n"
                               f"Verifique se você tem permissão de escrita em:\n{DIRETORIO_REDE}\n\n"
                               f"Erro: {str(e)}")
        except Exception as e:
            self.adicionar_log(f"❌ Erro ao salvar arquivo: {str(e)}")
            messagebox.showerror("Erro", f"Falha ao salvar arquivo: {str(e)}")
    
    def fechar_aplicacao(self):
        """Fecha a conexão com o banco e encerra a aplicação"""
        self.processando = False  # Sinalizar para parar processamento
        
        try:
            # Aguardar um pouco para processamento parar
            time.sleep(0.5)
            
            # Fechar conexão com o banco se existir
            if self.connection:
                self.connection.cursor.close()
                self.connection.connection.close()
                self.adicionar_log("🔒 Conexão com o banco fechada")
            
            # Encerrar a aplicação
            self.janela.quit()
            self.janela.destroy()
            
        except Exception as e:
            print(f"Erro ao fechar aplicação: {e}")
            self.janela.quit()
            self.janela.destroy()
    
    def executar(self):
        """Inicia a interface"""
        self.janela.mainloop()

# Executar a aplicação
if __name__ == "__main__":
    app = InterfaceProcessador()
    app.executar()