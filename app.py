import customtkinter as ctk

# Configuração da aparência

ctk.set_appearance_mode('dark')

# Criação das funções de funcionalidades
def validar_login():
    usuario = campo_usuario.get()
    senha = campo_senha.get()

    # Verificação
    if usuario == 'willian' and senha == '123456':
        resultado_login.configure(text='Login realizado com sucesso!', text_color='green')
    else:
        resultado_login.configure(text="Usuário ou senha incorretos!", text_color='red')


# Configuração da janela principal

app = ctk.CTk()
app.title("SISTEMA DE LOGIN")
app.geometry('300x300')

# Criação dos campos
# label do usuário
label_usuario = ctk.CTkLabel(app, text='Usuário')
label_usuario.pack(pady=10)

#entry do usuário
campo_usuario = ctk.CTkEntry(app, placeholder_text="Digite seu usuário")
campo_usuario.pack(pady=10)

# label da senha
label_senha = ctk.CTkLabel(app, text='Senha')
label_senha.pack(pady=10)

# entry da senha
campo_senha = ctk.CTkEntry(app, placeholder_text="Digite sua senha", show='*')
campo_senha.pack(pady=10)

#button
bota_login = ctk.CTkButton(app, text='Login', command=validar_login)
bota_login.pack(pady=10)

# campo feedback de login
resultado_login = ctk.CTkLabel(app, text='')
resultado_login.pack(pady=10)

# Iniciar a aplicação
app.mainloop()