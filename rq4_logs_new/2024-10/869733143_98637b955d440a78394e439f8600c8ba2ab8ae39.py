from flask import Flask, render_template, request, redirect, url_for
import os

class APP:
    def __init__ (self, import_name):
        self.app = Flask(import_name)
        self.app.secret_key = os.urandom(24)

        self.dados = []
        self.semanas()
        self.notas()
    
    def semanas (self):
        @self.app.route('/', methods=['GET', 'POST'])
        def semana1():
            return render_template('semana1.html')
        
        @self.app.route('/semana2', methods=['GET', 'POST'])
        def semana2():
            return render_template('semana2.html')
        
        @self.app.route('/semana3', methods=['GET', 'POST'])
        def semana3():
            return render_template('semana3.html')
        
        @self.app.route('/semana4', methods=['GET', 'POST'])
        def semana4():
            return render_template('semana4.html')

    def notas (self):
        @self.app.route('/cadastro-nota', methods=['GET', 'POST'])
        def cadastro_nota ():
            return render_template('nota.html')

        @self.app.route('/guarda-dados', methods=['GET', 'POST'])
        def guarda_dados ():
            name = request.form['event-name']
            date = request.form['event-date']
            description = request.form['event-description']

            evento = {"Evento": name, "Data": date, "Descrição": description}

            print(evento)
            self.dados.append(evento)
            
            with open('eventos.txt', 'w') as arquivo:
                arquivo.write(f"{name}\n")
                arquivo.write(f"{date}\n")
                arquivo.write(f"{description}\n")
                arquivo.write('\n')
            
            return redirect(url_for('semana1'))
    
    def run(self, **kwargs):
        # Método para rodar a aplicação
        self.app.run(**kwargs)

if __name__ == '__main__':
    app = APP(__name__)
    app.run(debug=True)