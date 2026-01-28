import customtkinter as ctk

# Configuração da aparência

ctk.set_appearance_mode('dark')

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
campo_senha = ctk.CTkEntry(app, placeholder_text="Digite sua senha")
campo_senha.pack(pady=10)

# Iniciar a aplicação
app.mainloop()