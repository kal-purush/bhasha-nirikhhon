from flask import Flask
from flask_jwt_extended import JWTManager
from flask_restx import Api
from flask_cors import CORS  
from werkzeug.middleware.proxy_fix import ProxyFix
from config import Config
from models import db, bcrypt, Livro, Usuario, UsuarioLivro
from routes import usuario_namespace, livros_namespace

app = Flask(__name__)
CORS(app)  
app.wsgi_app = ProxyFix(app.wsgi_app)
app.config.from_object(Config)
api = Api(
    app, 
    version='1.0', 
    title='API de Livros', 
    description='Uma API para gerenciar livros e avaliações', 
    doc='/swagger',
    security='Bearer Auth', 
    authorizations={
        'Bearer Auth': {
            'type': 'apiKey',
            'in': 'header',
            'name': 'Authorization',
            'description': 'Adicione o token JWT no formato: Bearer <token>'
        }
    }
    )

# Inicialização das extensões
db.init_app(app)
bcrypt.init_app(app)
jwt = JWTManager(app)

# Registrando os Namespaces diretamente no API
api.add_namespace(usuario_namespace)
api.add_namespace(livros_namespace)  

with app.app_context():
    db.create_all()


    if Livro.query.count() == 0:

        admin = Usuario(nome="Admin", email="admin@admin.com", senha="admin123", role="ADMIN")
        db.session.add(admin)
        db.session.commit()

        # Adicionar livros padrões
        livros_iniciais = [
            Livro(nome="Adagas do Universo", autor="Antonio Carlos", sinopse="Uma aventura épica no espaço, onde heróis improváveis se unem para salvar galáxias distantes. Um confronto entre os guerreiros das estrelas e uma ameaça que pode destruir toda a vida no universo.", avaliacao=0, cadastrado_por=admin.id),
            Livro(nome="O Império da Magia", autor="Rodrigo Cunha", sinopse="Em um reino onde magia é proibida, um grupo de rebeldes luta contra a tirania de um imperador impiedoso. Com poderes ancestrais e aliados inesperados, eles enfrentam perigos para libertar a terra das trevas.", avaliacao=0, cadastrado_por=admin.id),
            Livro(nome="Mistérios do Passado", autor="Rodrigo Cunha", sinopse="Um thriller psicológico, onde um detetive e uma psicanalista desvendam segredos obscuros de um caso não resolvido. À medida que se aprofundam nas sombras do passado, percebem que alguns mistérios nunca devem ser revelados.", avaliacao=0, cadastrado_por=admin.id),
            Livro(nome="A Chave de Ouro", autor="Isabela Souza", sinopse="Após a descoberta de uma chave ancestral, uma jovem é transportada para um mundo mágico. Nessa terra, ela deve resolver enigmas e enfrentar criaturas mitológicas para encontrar a verdade sobre sua origem e seu destino.", avaliacao=0, cadastrado_por=admin.id),
            Livro(nome="O Último Suspiro", autor="Carlos Silva", sinopse="Em um futuro distópico, a humanidade está à beira da extinção. Apenas um grupo de sobreviventes luta contra criaturas mutantes. A chave para a salvação pode estar em uma fórmula científica esquecida há muito tempo.", avaliacao=0, cadastrado_por=admin.id),
            Livro(nome="O Labirinto de Cristal", autor="Fernanda Oliveira", sinopse="Um arqueólogo encontra um mapa que o leva a um labirinto submerso, repleto de enigmas e artefatos antigos. Com a ajuda de uma misteriosa estranha, ele tenta descobrir os segredos de uma civilização perdida, enquanto enfrenta perigos mortais.", avaliacao=0, cadastrado_por=admin.id),
            Livro(nome="A Última Lua", autor="Marcos Lima", sinopse="Na última noite do calendário celestial, uma lua vermelha ilumina os céus. Um grupo de jovens se vê em uma jornada onde lendas e realidades se misturam, e eles terão que escolher entre salvar ou destruir o mundo que conhecem.", avaliacao=0, cadastrado_por=admin.id),
            Livro(nome="O Voo das Sombras", autor="Cláudia Reis", sinopse="Em uma cidade onde a noite nunca termina, uma jovem piloto de drones é chamada para uma missão secreta. À medida que ela se aproxima da verdade, ela descobre uma conspiração que pode mudar o destino do mundo inteiro.", avaliacao=0, cadastrado_por=admin.id),
            Livro(nome="Lágrimas de Fogo", autor="Lucas Pinto", sinopse="Quando uma cidade é destruída por um incêndio misterioso, um jovem busca vingança e justiça. Porém, ao descobrir o verdadeiro causador do desastre, ele é forçado a confrontar dilemas morais que abalam seus princípios.", avaliacao=0, cadastrado_por=admin.id),
            Livro(nome="O Eco da Mente", autor="Camila Souza", sinopse="Em um futuro onde a mente humana pode ser conectada à internet, um hacker encontra uma falha que permite acessar os pensamentos mais íntimos das pessoas. Mas quando ele começa a perder o controle, ele entra em uma batalha com a própria realidade.", avaliacao=0, cadastrado_por=admin.id)
        ]
        db.session.add_all(livros_iniciais)
        db.session.commit()

if __name__ == "__main__":
    app.run(debug=True, threaded=True)