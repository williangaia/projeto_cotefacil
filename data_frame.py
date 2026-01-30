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
            
        else:
            print("Falha na conexão com banco de dados")
    except Exception as e:
        print(f"Erro durante a execução: {e}")
    finally:
        if 'conexao' in locals():
            conexao.fechar_conexao()