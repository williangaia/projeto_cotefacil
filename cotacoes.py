import snorte
import time
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
    def __init__(self, numero_cotacao):
        self.numero_cotacao = numero_cotacao
        self.diretorio = Path(__file__).parent
        self.nome_arquivo = f"cotacao_{numero_cotacao}.csv"
        self.caminho_esqueleto = self.diretorio / self.nome_arquivo

    def criar_esqueleto(self):
        try:
            with open(self.caminho_esqueleto, 'w', newline='', encoding='utf-8') as arquivo:
                writer = csv.writer(arquivo, delimiter='\t')
                #Primeira linha - vazia
                writer.writerow(['','','','','',''])

                #Segunda linha - Número da cotação
                writer.writerow([f'Cotação: {self.numero_cotacao}','','','','',''])

                #Terceira Linha - CENTRAL-COMPRAS
                writer.writerow(['CENTRAL-COMPRAS','','','','',''])

                #Quarta linha - colunas
                writer.writerow(['Seq','EAN','Descrição','Emb.','Prazo','Vlr. Custo' ])
            
            print(f"Esqueleto salvo em {self.diretorio}")
        except Exception as e:
            print(f"Erro ao criar esqueleto do CSV: {e}")
        
    







# BLOCO PRINCIPAL
"""
conexao = ConexaoBD()
conexao.verifica_conexao()
time.sleep(5)
conexao.fechar_conexao()
"""

esqueleto = CriarCSV(202261)
esqueleto.criar_esqueleto()