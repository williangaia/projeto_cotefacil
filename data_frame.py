import snorte
from pathlib import Path
import csv
import pandas as pd

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

class CotacaoRepository():
    def __init__(self, numero_cotacao, conexao: ConexaoBD):
        self.numero_cotacao = numero_cotacao
        self.conexao = conexao
        
    def buscar_produtos_cotacao(self):
        cursor = self.conexao.conexao.cursor

        consulta_produtos_cotacao = f"""
        SELECT
            SEQPRODUTO AS seq,
            CODIGOEAN AS ean,
            DESCRICAO AS descricao,
            EMBALAGEM AS embalagem,
            QTDEMBALAGEM AS qtd_embalagem
        FROM MRLV_LISTACOTACAO C
        WHERE C.SEQCOTACAO = {self.numero_cotacao}
        """

        cursor.execute(consulta_produtos_cotacao)
        colunas = [desc[0] for desc in cursor.description]
        linhas = cursor.fetchall()

        df = pd.DataFrame(linhas, columns=colunas)
        df.columns = df.columns.str.lower()

        return df
    
    def buscar_atacadistas_cotacao(self) -> pd.DataFrame:
        cursor = self.conexao.conexao.cursor

        consulta_atacadistas_cotacao = f"""
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

        cursor.execute(consulta_atacadistas_cotacao)
        colunas = [desc[0] for desc in cursor.description]
        linhas = cursor.fetchall()

        df = pd.DataFrame(linhas, columns=colunas)
        df.columns = df.columns.str.lower()

        return df

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

                # Ignorar primeira linha
                if tipo == "1":
                    continue

                # CNPJ do fornecedor
                if tipo == "2":
                    cnpj_atual = campos[1]
                    precos_por_fornecedor[cnpj_atual] = {}
                    continue

                # Produtos do fornecedor
                if tipo == "3" and cnpj_atual:
                    ean = campos[1]
                    preco = campos[4]
                    precos_por_fornecedor[cnpj_atual][ean] = preco

                # Acabou o fornecedor
                if tipo == "4":
                    cnpj_atual = None

                # Fim do arquivo
                if tipo == "5":
                    break

        return precos_por_fornecedor
    
class CotacaoDataFrameService:

    @staticmethod
    def montar_df_cotacao_fornecedor(df_cotacao: pd.DataFrame, precos_fornecedor: dict) -> pd.DataFrame:
        df = df_cotacao.copy()

        df["Vlr. Custo"] = df["ean"].map(precos_fornecedor).fillna("0,00")

        return df
    
    @staticmethod ####
    def preparar_df_final( df: pd.DataFrame) -> pd.DataFrame: ####
        df_final = df.copy()

        df_final["Emb."] = (
            df_final["embalagem"].astype(str) + "-" + df_final["qtd_embalagem"].astype(str) #####
        )

        #Prazo - valor fixo no momento
        df_final["Prazo"] = 30

        # Formatar o preço - Vlr. Custo
        df_final["Vlr. Custo"] = (
            df_final["Vlr. Custo"].astype(str).str.replace(".", ",", regex=False)
        )

        # Formatar colinas do df
        df_final = df_final[
            ["seq", "ean", "descricao", "Emb.", "Prazo", "Vlr. Custo"]
        ]

        return df_final

class CotacaoExporter:

    @staticmethod ####
    def salvar_cotacao_csv(df: pd.DataFrame, caminho: Path, numero_cotacao: int):

        with open(caminho, mode="w", newline="", encoding="utf-8") as arquivo:
            writer = csv.writer(arquivo)

            # Cabeçalho Consinco
            writer.writerow(["", "", "", "", "", ""])
            writer.writerow([f"Cotação: {numero_cotacao}", "", "", "", "", ""])
            writer.writerow(["CENTRAL-COMPRAS", "", "", "", "", ""])
            writer.writerow(df.columns.tolist())

            for _, row in df.iterrows():
                writer.writerow(row.tolist())
    
    @staticmethod ####
    def salvar_cotacao_xlsx(caminho: Path, dfs_por_fornecedor: dict[str, pd.DataFrame]):

        with pd.ExcelWriter(caminho, engine="xlsxwriter") as writer:
            for nome_razao, df in dfs_por_fornecedor.items():
                aba = nome_razao[:31]
                df.to_excel(writer, sheet_name=aba, index=False) ####


# BLOCO PRINCIPAL

if __name__ == "__main__":
    
    conexao = ConexaoBD()

    try:
        if not conexao.verifica_conexao():
            raise RuntimeError("Falha na conexão com o banco!")
        
        repositorio = CotacaoRepository(202280, conexao)
        df_cotacao = repositorio.buscar_produtos_cotacao()
        df_atacadista = repositorio.buscar_atacadistas_cotacao()

        parser = TxtCotacaoParser(
            Path(__file__).parent / "PEDIDO_13808028_29012026_0904532720.txt"
        )
        precos = parser.extrair_precos()

        dfs_xlsx = {}

        for _, atac in df_atacadista.iterrows():
            cnpj = atac["cnpj_completo"]
            nome_razao = atac["nomerazao"]

            df_fornecedor = CotacaoDataFrameService.montar_df_cotacao_fornecedor(
                df_cotacao,
                precos.get(cnpj, {})
            )
            
            df_final = CotacaoDataFrameService.preparar_df_final(df_fornecedor)

            caminho_csv = (
                Path(__file__).parent / f"Cotação{repositorio.numero_cotacao}_{nome_razao}.csv"
            )

            CotacaoExporter.salvar_cotacao_csv(
                df_final,
                caminho_csv,
                repositorio.numero_cotacao
            )

            dfs_xlsx[nome_razao] = df_final

        caminho_xlsx = (
            Path(__file__).parent / f"Cotacao{repositorio.numero_cotacao}.xlsx"
        )

        CotacaoExporter.salvar_cotacao_xlsx(caminho_xlsx, dfs_xlsx)
        
    finally:
        if 'conexao' in locals():
            conexao.fechar_conexao()