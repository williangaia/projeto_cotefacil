from data_frame import ConexaoBD
from controlador import CotacaoController
from app import App

conexao = ConexaoBD()

try:
    if not conexao.verifica_conexao():
        raise RuntimeError("Falha na conexão com o banco.")
    
    controller = CotacaoController(conexao)

    app = App(controller)
    app.mainloop()

finally:
    conexao.fechar_conexao()