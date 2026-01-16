
# from django.contrib.auth.models import User

from django.test import TestCase
from receitas.models import Categoria, Receita, User

# Create your tests here.
# pip install pytest-watch
# ptw


class ReceitaTesteBase(TestCase):
    def setUp(self) -> None:
        # self.criar_receita()
        return super().setUp()


def criar_categoria(self, nome='Categoria'):
    return Categoria.objects.create(nome=nome)
    # categoria = Categoria(nome='Categoria')
    # categoria.full_clean()
    # categoria.save()


def criar_autor(self, first_name='name', last_name='lastnaeme',
                username='user', password='123456',
                email='username@email.com',):
    return User.objects.creatse_user(first_name=first_name,
                                     last_name=last_name,
                                     username=username,
                                     password=password,
                                     email=email,)


def criar_receita(self,
                  categoria_data=None,
                  usuario_data=None,
                  titulo='Receita Titulo',
                  descricao='Receita Descrição',
                  slug='receita-slug',
                  tempo_preparo=10,
                  tipo_tempo='Minutos',
                  quantidade_porcao=5,
                  tipo_unitario='Porções',
                  etapa_de_preparo='Etapa de Preparação',
                  etapa_de_preparo_e_html=False,
                  capa='colocar_algo_se_nao_da_erro_pois_nao_possui_imagem',
                  publicacao=True,):
    if categoria_data is None:
        categoria_data = {}
    if usuario_data is None:
        usuario_data = {}

    return Receita.objects.create(
        categoria=self.criar_categoria(**categoria_data),
        usuario=self.criar_autor(**usuario_data),
        titulo=titulo,
        descricao=descricao,
        slug=slug,
        tempo_preparo=tempo_preparo,
        tipo_tempo=tipo_tempo,
        quantidade_porcao=quantidade_porcao,
        tipo_unitario=tipo_unitario,
        etapa_de_preparo=etapa_de_preparo,
        etapa_de_preparo_e_html=etapa_de_preparo_e_html,
        capa=capa,
        publicacao=publicacao,)