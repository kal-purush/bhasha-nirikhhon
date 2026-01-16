import pytest
from src.app import app

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_home_route(client):
    """Teste se a rota home retorna 200 e contém conteúdo esperado"""
    response = client.get('/')
    assert response.status_code == 200
    content = response.data.decode('utf-8')
    assert 'Bem-vindo!' in content
    assert 'Página Inicial' in content
    assert '<html' in content, "A resposta deve conter HTML"

def test_sobre_route(client):
    """Teste se a rota sobre retorna 200 e contém conteúdo esperado"""
    response = client.get('/sobre')
    assert response.status_code == 200
    content = response.data.decode('utf-8')
    assert 'Sobre' in content
    assert 'Sobre Nós' in content
    assert response.content_type == 'text/html; charset=utf-8'

def test_contatos_route(client):
    """Teste se a rota contatos retorna 200 e contém conteúdo esperado"""
    response = client.get('/contatos')
    assert response.status_code == 200
    content = response.data.decode('utf-8')
    assert 'Fale Conosco' in content
    assert 'contato@exemplo.com' in content
    assert response.headers.get('Content-Type') == 'text/html; charset=utf-8'

def test_usuario_route(client):
    """Teste se a rota dinâmica do usuário retorna 200 e contém conteúdo esperado"""
    nomes = ['TestUser', 'João', 'Maria']
    for nome in nomes:
        response = client.get(f'/user/{nome}')
        assert response.status_code == 200
        content = response.data.decode('utf-8')
        assert f'Olá, {nome}!' in content
        assert f'Página de {nome}' in content

def test_rota_inexistente(client):
    """Teste se uma rota inexistente retorna 404"""
    response = client.get('/pagina-que-nao-existe')
    assert response.status_code == 404