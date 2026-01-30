import snorte
from pathlib import Path
import csv

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

class CriarCSV():
    def __init__(self, numero_cotacao, conexao: ConexaoBD):
        self.numero_cotacao = numero_cotacao
        self.diretorio = Path(__file__).parent
        self.nome_arquivo = f"cotacao_{numero_cotacao}.csv"
        self.caminho_esqueleto = self.diretorio / self.nome_arquivo
        self.conexao = conexao

    def criar_esqueleto(self):
        try:
            produtos = self.buscar_produtos_cotacao()

            with open(self.caminho_esqueleto, 'w', newline='', encoding='utf-8') as arquivo:
                writer = csv.writer(arquivo, delimiter=',')
                #Primeira linha - vazia
                writer.writerow(['','','','','',''])

                #Segunda linha - Número da cotação
                writer.writerow([f'Cotação: {self.numero_cotacao}','','','','',''])

                #Terceira Linha - CENTRAL-COMPRAS
                writer.writerow(['CENTRAL-COMPRAS','','','','',''])

                #Quarta linha - colunas
                writer.writerow(['Seq','EAN','Descrição','Emb.','Prazo','Vlr. Custo' ])

                #Preencher com os produtos
                for produto in produtos:
                    seq = produto['SEQPRODUTO']
                    ean = produto['CODIGOEAN']
                    descricao = produto['DESCRICAO']
                    embalagem = f"{produto['EMBALAGEM']}-{produto['QTDEMBALAGEM']}"
                    prazo = 30
                    valor_custo = ''
                    writer.writerow([seq, ean, descricao, embalagem, prazo, valor_custo])

            #depois tirar esse print - só para teste
            print(f"CSV criado e preenchido em {self.caminho_esqueleto}")
            print(f"Total de {len(produtos)} produtos adicionados")
            print(f"Esqueleto salvo em {self.diretorio}")
            return True
        
        except Exception as e:
            print(f"Erro ao criar esqueleto do CSV: {e}")
            return False
        
    def buscar_produtos_cotacao(self):
        cursor = self.conexao.conexao.cursor

        consulta_produtos_cotacao = f"""
        SELECT SEQCOTACAO, SEQPRODUTO, CODIGOEAN, DESCRICAO, EMBALAGEM, QTDEMBALAGEM
        FROM MRLV_LISTACOTACAO C
        WHERE C.SEQCOTACAO = {self.numero_cotacao}
        """

        cursor.execute(consulta_produtos_cotacao)
        colunas = [desc[0] for desc in cursor.description]
        linhas = cursor.fetchall()

        produtos =[]
        for linha in linhas:
            produto = dict(zip(colunas, linha))
            produtos.append(produto)
        
        return produtos


# BLOCO PRINCIPAL

if __name__ == "__main__":
    try:
        conexao = ConexaoBD()
        if conexao.verifica_conexao():
            esqueleto = CriarCSV(202280, conexao)
            esqueleto.criar_esqueleto()
        else:
            print("Falha na conexão com banco de dados")
    except Exception as e:
        print(f"Erro durante a execução: {e}")
    finally:
        if 'conexao' in locals():
            conexao.fechar_conexao()