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
            SEQPRODUTO AS Seq,
            CODIGOEAN AS EAN,
            DESCRICAO AS Descrição,
            EMBALAGEM,
            QTDEMBALAGEM
        FROM MRLV_LISTACOTACAO C
        WHERE C.SEQCOTACAO = {self.numero_cotacao}
        """

        cursor.execute(consulta_produtos_cotacao)
        colunas = [desc[0] for desc in cursor.description]
        linhas = cursor.fetchall()

        df = pd.DataFrame(linhas, columns=colunas)

        #Conctenar as duas colunas em uma chamada Emb.
        #df["Emb."] = df["EMBALAGEM"].astype(str) + "-" + df["QTDEMBALAGEM"].astype(str)

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

        return df

class TxtCotacaoParser:
    def __init__(self, caminho_arquivo: Path):
        self.caminho_arquivo = caminho_arquivo

    def extrair_precos(self) -> dict:
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
    
    @staticmethod
    def montar_df_cotacao_fornecedor(df_cotacao: pd.DataFrame, precos_fornecedor: dict) -> pd.DataFrame:
        df = df_cotacao.copy()

        df["Vlr. Custo"] = df["EAN"].map(precos_fornecedor).fillna("0,00")

        return df

# BLOCO PRINCIPAL

if __name__ == "__main__":
    try:
        conexao = ConexaoBD()
        if conexao.verifica_conexao():
            repositorio = CotacaoRepository(202280, conexao)
            df_cotacao = repositorio.buscar_produtos_cotacao()
            df_atacadistas = repositorio.buscar_atacadistas_cotacao()

            print("Produtos: ")
            print(df_cotacao.head())

            print("\nAtacadistas: ")
            print(df_atacadistas.head())

            diretorio_pai = Path(__file__).parent
            arquivo = Path("PEDIDO_13808028_29012026_0904532720.txt")
            caminho_txt = diretorio_pai / arquivo

            parser = TxtCotacaoParser(caminho_txt)
            precos = parser.extrair_precos()

            for cnpj, precos_fornecedor in precos.items():
                df_fornecedor = TxtCotacaoParser.montar_df_cotacao_fornecedor(
                    df_cotacao,
                    precos_fornecedor
                )

                print(f"\nFornecedor {cnpj}")
                print(df_fornecedor.head())
            
        else:
            print("Falha na conexão com banco de dados")
    except Exception as e:
        print(f"Erro durante a execução: {e}")
    finally:
        if 'conexao' in locals():
            conexao.fechar_conexao()