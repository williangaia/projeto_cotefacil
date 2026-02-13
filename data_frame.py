# data_frame.py - MODEL
import snorte
from pathlib import Path
import csv
import pandas as pd
from abc import ABC, abstractmethod

class ConexaoBD:
    def __init__(self):
        try:
            self.conexao = snorte.Snorte()
            print("Conexão com o banco inicializada!")
        except Exception:
            self.conexao = None
            print(f"Erro ao inicializar conexão.")

    def verifica_conexao(self) -> bool:
        if self.conexao and self.conexao.connection:
            print("Sucesso na conexão!")
            return True
        else:
            print("Erro ao conectar!")
            return False

    def fechar_conexao(self):
        if not self.conexao:
            return
        
        try:
            if self.conexao.cursor:
                self.conexao.cursor.close()
            if self.conexao.connection:
                self.conexao.connection.close()
            print("Conexão com o banco encerrada!")
        except Exception as e:
            print(f"Erro inesperado ao fechar a conexão: {e}", exc_info=True)

# ============ PADRÃO REPOSITORY ============
class BaseRepository:
    def __init__(self, conexao: ConexaoBD):
        self.conexao = conexao
    
    def _executar_consulta(self, query: str) -> pd.DataFrame:
        cursor = self.conexao.conexao.cursor
        cursor.execute(query)
        colunas = [desc[0] for desc in cursor.description]
        linhas = cursor.fetchall()
        
        df = pd.DataFrame(linhas, columns=colunas)
        df.columns = df.columns.str.lower()
        return df

class CotacaoRepository(BaseRepository):
    def __init__(self, numero_cotacao: int, conexao: ConexaoBD):
        super().__init__(conexao)
        self.numero_cotacao = numero_cotacao
        
    def buscar_produtos_cotacao(self) -> pd.DataFrame:
        query = f"""
        SELECT
            SEQPRODUTO AS seq,
            CODIGOEAN AS ean,
            DESCRICAO AS descricao,
            EMBALAGEM AS embalagem,
            QTDEMBALAGEM AS qtd_embalagem
        FROM MRLV_LISTACOTACAO C
        WHERE C.SEQCOTACAO = {self.numero_cotacao}
        """
        return self._executar_consulta(query)
    
    def buscar_atacadistas_cotacao(self) -> pd.DataFrame:
        query = f"""
        SELECT
            M.SEQATACCOTACAO,
            M.SEQATACADISTA,
            CONCAT(
                LPAD(P.NROCGCCPF, 12, '0'),
                LPAD(P.DIGCGCCPF, 2, '0')
            ) AS CNPJ_COMPLETO,
            P.NOMERAZAO
        FROM MRL_ATACCOTADO M
        INNER JOIN GE_PESSOA P
            ON P.SEQPESSOA = M.SEQATACADISTA
        WHERE M.SEQATACCOTACAO = {self.numero_cotacao}
        """
        return self._executar_consulta(query)
    
    def buscar_cotacao_cotefacil_por_filial(self) -> pd.DataFrame:
        query = f"""
        SELECT
            A.NROEMPRESA,
            (Select max(c.codacesso)
            From map_prodcodigo c
            Where c.seqproduto = a.seqproduto
                and c.tipcodigo = 'E'
                and c.qtdembalagem = 1) AS EAN,

            Trunc(a.qtdpedida) AS QUANTIDADE,

            (Select max(c.codacesso)
            From map_prodcodigo c
            Where c.seqproduto = a.seqproduto
                and c.tipcodigo = 'E'
                and c.qtdembalagem = 1) AS EAN2,

            p.desccompleta as DESCRICAO,
            a.marca

        FROM mac_gercompraitem a,
            map_produto p

        WHERE a.seqproduto = p.seqproduto
        and a.qtdpedida <> 0
        and a.seqgercompra = {self.numero_cotacao}
        """

        return self._executar_consulta(query)

class TxtCotacaoParser:
    def __init__(self, caminho_arquivo: Path):
        self.caminho_arquivo = caminho_arquivo

    def extrair_precos(self) -> dict[str, dict[str, str]]:
        precos_por_fornecedor = {}
        cnpj_atual = None

        with open(self.caminho_arquivo, encoding="utf-8") as arquivo:
            for linha in arquivo:
                linha = linha.strip()
                if not linha:
                    continue

                campos = linha.split(";")
                tipo = campos[0]

                if tipo == "1":
                    continue

                if tipo == "2":
                    cnpj_atual = campos[1]
                    precos_por_fornecedor[cnpj_atual] = {}
                    continue

                if tipo == "3" and cnpj_atual:
                    ean = campos[1]
                    preco = campos[4]
                    precos_por_fornecedor[cnpj_atual][ean] = preco

                if tipo == "4":
                    cnpj_atual = None

                if tipo == "5":
                    break

        return precos_por_fornecedor

# ============ PADRÃO STRATEGY ============
class EstrategiaProcessamento(ABC):
    @abstractmethod
    def processar(self, repositorio: CotacaoRepository, **kwargs) -> dict:
        pass

class EstrategiaConsinco(EstrategiaProcessamento):
    def processar(self, repositorio: CotacaoRepository, **kwargs) -> dict:
        if 'caminho_txt' not in kwargs:
            raise ValueError("Estratégia Consinco requer arquivo TXT")
        
        from data_frame import TxtCotacaoParser
        parser = TxtCotacaoParser(kwargs['caminho_txt'])
        precos = parser.extrair_precos()
        
        df_cotacao = repositorio.buscar_produtos_cotacao()
        df_atacadistas = repositorio.buscar_atacadistas_cotacao()
        
        resultados = {}
        for _, atac in df_atacadistas.iterrows():
            cnpj = atac["cnpj_completo"]
            nome_razao = atac["nomerazao"]
            
            df_fornecedor = self._montar_df_fornecedor(df_cotacao, precos.get(cnpj, {}))
            df_final = self._preparar_df_final(df_fornecedor)
            
            resultados[nome_razao] = {
                'df': df_final,
                'cnpj': cnpj
            }
        
        return {
            'tipo': 'consinco',
            'resultados': resultados,
            'df_atacadistas': df_atacadistas
        }
    
    def _montar_df_fornecedor(self, df_cotacao: pd.DataFrame, precos_fornecedor: dict) -> pd.DataFrame:
        df = df_cotacao.copy()
        df["Vlr. Custo"] = df["ean"].map(precos_fornecedor).fillna("0,00")
        return df
    
    def _preparar_df_final(self, df: pd.DataFrame) -> pd.DataFrame:
        df_final = df.copy()
        df_final["Emb."] = (
            df_final["embalagem"].astype(str) + "-" + df_final["qtd_embalagem"].astype(str)
        )
        df_final["Prazo"] = 30
        df_final["Vlr. Custo"] = (
            df_final["Vlr. Custo"].astype(str).str.replace(".", ",", regex=False)
        )
        df_final = df_final[
            ["seq", "ean", "descricao", "Emb.", "Prazo", "Vlr. Custo"]
        ]
        return df_final

class EstrategiaCotefacil(EstrategiaProcessamento):
    def processar(self, repositorio: CotacaoRepository, **kwargs) -> dict:

        df = repositorio.buscar_cotacao_cotefacil_por_filial()

        if df.empty:
            raise ValueError("Nenhum dado encontrado para esta cotação.")

        resultados = {}

        # Agrupar por filial
        for nroempresa, df_filial in df.groupby("nroempresa"):

            df_filial = df_filial.copy()

            # Manter somente as colunas do layout
            df_filial = df_filial[
                ["ean", "quantidade", "ean2", "descricao", "marca"]
            ]

            # Garantir ordem correta
            df_filial = df_filial.rename(columns={"ean2": "ean_duplicado"})

            resultados[nroempresa] = df_filial

        return {
            "tipo": "cotefacil",
            "resultados": resultados
        }

# ============ EXPORTERS ============
class BaseExporter(ABC):
    @abstractmethod
    def exportar(self, dados, caminho: Path, **kwargs):
        pass

class CSVExporterConsinco(BaseExporter):
    def exportar(self, dados, caminho: Path, **kwargs):
        df = dados['df']
        numero_cotacao = kwargs.get('numero_cotacao')
        
        with open(caminho, mode="w", newline="", encoding="utf-8-sig") as arquivo:
            writer = csv.writer(arquivo, delimiter=";")
            writer.writerow([])
            writer.writerow([f"Cotação: {numero_cotacao}"])
            writer.writerow(["CENTRAL-COMPRAS"])
            writer.writerow(["Seq", "EAN", "Descrição", "Emb.", "Prazo", "Vlr. Custo"])
            
            for _, row in df.iterrows():
                writer.writerow([
                    row["seq"],
                    row["ean"],
                    row["descricao"],
                    row["Emb."],
                    row["Prazo"],
                    row["Vlr. Custo"]
                ])

class CSVExporterCotefacil(BaseExporter):
    def exportar(self, dados, caminho: Path, **kwargs):
        df = dados['df_cotacao']
        
        with open(caminho, mode="w", newline="", encoding="utf-8-sig") as arquivo:
            writer = csv.writer(arquivo, delimiter=";")
            # SEM cabeçalho, apenas dados
            for _, row in df.iterrows():
                writer.writerow([
                    row["ean"],           # Primeira coluna EAN
                    row["quantidade"],    # QUANTIDADE
                    row["ean_duplicado"], # Segunda coluna EAN (duplicada)
                    row["descricao"],     # DESCRICAO
                    row["marca"]          # MARCA
                ])

class XLSXExporter(BaseExporter):
    def exportar(self, dados, caminho: Path, **kwargs):
        resultados = dados['resultados']
        
        print("Gerando XLSX em:", caminho)
        with pd.ExcelWriter(caminho, engine="xlsxwriter") as writer:
            for nome_razao, info in resultados.items():
                df = info['df']
                df_export = df.rename(columns={
                    "seq": "Seq",
                    "ean": "EAN",
                    "descricao": "Descrição",
                    "Emb.": "Emb.",
                    "Prazo": "Prazo",
                    "Vlr. Custo": "Vlr. Custo"
                })
                aba = nome_razao[:31]
                df_export.to_excel(writer, sheet_name=aba, index=False)
        print("XLSX gerado com sucesso")

# ============ FACTORY ============
class ProcessadorFactory:
    @staticmethod
    def criar_processador(tipo: str) -> EstrategiaProcessamento:
        if tipo == "consinco":
            return EstrategiaConsinco()
        elif tipo == "cotefacil":
            return EstrategiaCotefacil()
        else:
            raise ValueError(f"Tipo de processador desconhecido: {tipo}")
    
    @staticmethod
    def criar_exporter(tipo: str) -> BaseExporter:
        if tipo == "consinco_csv":
            return CSVExporterConsinco()
        elif tipo == "cotefacil_csv":
            return CSVExporterCotefacil()
        elif tipo == "consinco_xlsx":
            return XLSXExporter()
        else:
            raise ValueError(f"Tipo de exporter desconhecido: {tipo}")