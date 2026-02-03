from pathlib import Path
from data_frame import CotacaoRepository, TxtCotacaoParser, CotacaoDataFrameService, CotacaoExporter
import re

class CotacaoController:

    def __init__(self, conexao):
        self.conexao = conexao

    def nome_arquivo_seguro(self, texto: str) -> str:
        # Remove caracteres invisíveis (quebra de linha, tab, etc)
        texto = re.sub(r"[\r\n\t]", " ", texto)

        # Remove caracteres inválidos para Windows
        texto = re.sub(r"[\\/:*?\"<>|]", "", texto)

        # Normaliza espaços
        texto = re.sub(r"\s+", " ", texto).strip()

        # Remove ponto ou espaço no final (Windows não aceita)
        texto = texto.rstrip(". ")

        return texto


    def processar_cotacao(
        self,
        numero_cotacao: int,
        caminho_txt: Path,
        pasta_saida: Path
    ):
        pasta_saida = Path(pasta_saida).resolve()
        pasta_saida.mkdir(parents=True, exist_ok=True)


        if not caminho_txt.exists():
            raise FileNotFoundError("Arquivo TXT não encontrado")

        if not pasta_saida.exists():
            raise FileNotFoundError("Pasta de saída inválida")

        
        repositorio = CotacaoRepository(numero_cotacao, self.conexao)

        df_cotacao = repositorio.buscar_produtos_cotacao()
        df_atacadistas = repositorio.buscar_atacadistas_cotacao()

        parser = TxtCotacaoParser(caminho_txt)
        precos = parser.extrair_precos()

        dfs_xlsx = {}

        for _, atac in df_atacadistas.iterrows():
            cnpj = atac["cnpj_completo"]
            nome_razao = atac["nomerazao"]

            df_fornecedor = CotacaoDataFrameService.montar_df_cotacao_fornecedor(
                df_cotacao,
                precos.get(cnpj, {})
            )

            df_final = CotacaoDataFrameService.preparar_df_final(df_fornecedor)

            nome_razao_limpo = self.nome_arquivo_seguro(nome_razao)

            caminho_csv = (
                pasta_saida / f"Cotação{repositorio.numero_cotacao}_{nome_razao_limpo}.csv"
            )

            caminho_csv.parent.mkdir(parents=True, exist_ok=True)

            ##TESTE
            print("NOME ORIGINAL:", repr(nome_razao))
            print("NOME LIMPO:", repr(nome_razao_limpo))
            print("CAMINHO:", repr(str(caminho_csv)))


            CotacaoExporter.salvar_cotacao_csv(
                df_final,
                caminho_csv,
                repositorio.numero_cotacao
            )

            dfs_xlsx[nome_razao] = df_final

        caminho_xlsx = pasta_saida / f"Cotacao{numero_cotacao}.xlsx"
        CotacaoExporter.salvar_cotacao_xlsx(caminho_xlsx, dfs_xlsx)