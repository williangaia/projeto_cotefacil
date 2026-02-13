# controlador.py - CONTROLLER
from pathlib import Path
from data_frame import CotacaoRepository, ProcessadorFactory
import re

class CotacaoController:

    def __init__(self, conexao):
        self.conexao = conexao

    def nome_arquivo_seguro(self, texto: str) -> str:
        texto = re.sub(r"[\r\n\t]", " ", texto)
        texto = re.sub(r"[\\/:*?\"<>|]", "", texto)
        texto = re.sub(r"\s+", " ", texto).strip()
        texto = texto.rstrip(". ")
        return texto

    def processar_cotacao(
        self,
        numero_cotacao: int,
        tipo_layout: str,  # "consinco" ou "cotefacil"
        caminho_txt: Path = None,
        pasta_saida: Path = None
    ):
        # Validações básicas
        if tipo_layout == "consinco" and not caminho_txt:
            raise ValueError("Layout Consinco requer arquivo TXT")
        
        if not pasta_saida:
            pasta_saida = Path.cwd() / "output"
        
        pasta_saida = Path(pasta_saida).resolve()
        pasta_saida.mkdir(parents=True, exist_ok=True)

        # Cria repositório
        repositorio = CotacaoRepository(numero_cotacao, self.conexao)
        
        # Factory para criar o processador correto
        processador = ProcessadorFactory.criar_processador(tipo_layout)
        
        # Processa de acordo com a estratégia
        if tipo_layout == "consinco":
            dados_processados = processador.processar(
                repositorio, 
                caminho_txt=caminho_txt
            )
            self._exportar_layout_consinco(
                dados_processados, 
                numero_cotacao, 
                pasta_saida
            )
        else:  # cotefacil
            dados_processados = processador.processar(repositorio)
            self._exportar_layout_cotefacil(
                dados_processados, 
                numero_cotacao, 
                pasta_saida
            )

    def _exportar_layout_consinco(self, dados, numero_cotacao: int, pasta_saida: Path):
        resultados = dados['resultados']
        df_atacadistas = dados['df_atacadistas']
        
        dfs_xlsx = {}
        
        for _, atac in df_atacadistas.iterrows():
            nome_razao = atac["nomerazao"]
            info = resultados.get(nome_razao)
            
            if not info:
                continue
                
            nome_razao_limpo = self.nome_arquivo_seguro(nome_razao)
            caminho_csv = pasta_saida / f"Cotação{numero_cotacao}_{nome_razao_limpo}.csv"
            
            # Exporta CSV
            exporter_csv = ProcessadorFactory.criar_exporter("consinco_csv")
            exporter_csv.exportar(
                {'df': info['df']}, 
                caminho_csv, 
                numero_cotacao=numero_cotacao
            )
            
            dfs_xlsx[nome_razao] = info['df']
        
        # Exporta XLSX
        if dfs_xlsx:
            caminho_xlsx = pasta_saida / f"Cotacao{numero_cotacao}.xlsx"
            exporter_xlsx = ProcessadorFactory.criar_exporter("consinco_xlsx")
            exporter_xlsx.exportar(
                {'resultados': {k: {'df': v} for k, v in dfs_xlsx.items()}}, 
                caminho_xlsx
            )

    def _exportar_layout_cotefacil(self, dados, numero_cotacao: int, pasta_saida: Path):

        resultados = dados["resultados"]

        exporter = ProcessadorFactory.criar_exporter("cotefacil_csv")

        for nroempresa, df_filial in resultados.items():

            caminho_csv = pasta_saida / f"Cotacao{numero_cotacao}_Loja{nroempresa}.csv"

            exporter.exportar(
                {"df_cotacao": df_filial},
                caminho_csv
            )

            print(f"Arquivo gerado: {caminho_csv}")