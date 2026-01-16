from sqlalchemy import create_engine

engine = create_engine('sqlite:///meubanco.db', echo = True)


### sqlite é o dialeto em que irei comunicar
###  EXEMPLO DE URL PARA POSTGRESSQL > postgresql+pg8000://dbuser:kx%40jj5%2Fg@pghost10/appdb;
## Usuário e senha sempre ficam em um arquivo de senhas separados,o exemplo é apenas para estudo de exemplo da lib ; 
### AQUI postgresql é o dialeto, pg8000 o driver, dbuser o usuário, kx%40jj5%2Fg a senha, pghost10 o host e appdb o db. 

print("Conexão com SQLite estabelecida")

from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String

Base = declarative_base()

class Usuario(Base):
    __tablename__ = 'usuarios' ## Aqui é o nome do db 
    
    ## Aqui, quando denominamos o tipo, estamos fazendo o Schema do Db, o SQLAlchemy faz a relação de Type para Schema.
    id = Column(Integer, primary_key=True)
    nome = Column(String)
    idade = Column(Integer)

# Criar as tabelas no banco de dados
Base.metadata.create_all(engine)  ## aqui criamos as tabelas/colunas

### CREATE TABLE usuarios (
###        id INTEGER NOT NULL, 
###        nome VARCHAR, 
###        idade INTEGER, 
###        PRIMARY KEY (id)
###)


### Aqui importamos um novo dado, não é a melhor maneira convencional, mas para entendimento e estudo é útil 
from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)
session = Session()

novo_usuario = Usuario(nome='João', idade=28)
session.add(novo_usuario)
session.commit()

print("Usuário inserido com sucesso.")

usuario = session.query(Usuario).filter_by(nome='João').first()
print(f"Usuário encontrado: {usuario.nome}, Idade: {usuario.idade}")

from sqlalchemy.orm import sessionmaker
# assumindo que engine já foi criado

Session = sessionmaker(bind=engine)
session = Session()

try:
    novo_usuario = Usuario(nome='Ana', idade=25)
    session.add(novo_usuario)
    session.commit()
except:
    session.rollback()
    raise
finally:
    session.close()
    
### Podemos também realizar o exemplo acima com with, ele abstrais algumas coisas, mas é importante entender o que é feito e quais as maneiras de fazer

from sqlalchemy.orm import sessionmaker, Session
# assumindo que engine já foi criado

Session = sessionmaker(bind=engine)

with Session() as session:
    novo_usuario = Usuario(nome='Ana_com_with', idade=25)
    session.add(novo_usuario)
    
    # O commit é feito automaticamente aqui, se não houver exceções
    # O rollback é automaticamente chamado se uma exceção ocorrer
    # A sessão é fechada automaticamente ao sair do bloco with