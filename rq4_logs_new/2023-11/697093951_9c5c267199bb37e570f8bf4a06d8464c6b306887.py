from ..models import Funcionario

def cadastrar_funcionario(funcionario):
    Funcionario.objects.create(nome=funcionario.nome, cpf=funcionario.cpf, endereco=funcionario.endereco,
                            telefone=funcionario.telefone, usuario=funcionario.usuario)
    

def listar_funcionario(usuario):
    # igual a select * from app_doador order by nome
    return Funcionario.objects.all().order_by('nome')

def listar_funcionario_id(id):
    return Funcionario.objects.get(id=id)


# recebe o funcionario já inserido no bd e o novo
def editar_funcionario(funcionario_bd, novo_funcionario):
    funcionario_bd.nome = novo_funcionario.nome
    funcionario_bd.cpf = novo_funcionario.cpf
    funcionario_bd.endereco = novo_funcionario.endereco
    funcionario_bd.telefone=novo_funcionario.telefone
    funcionario_bd.usuario= novo_funcionario.usuario
    funcionario_bd.save(force_update=True)


def excluir_funcionario(funcionario_bd):
    funcionario_bd.delete()