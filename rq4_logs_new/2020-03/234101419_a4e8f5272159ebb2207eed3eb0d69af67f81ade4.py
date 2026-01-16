from unittest.mock import Mock, patch

import pytest

from sme_uniforme_apps.proponentes.services import atualiza_coordenadas_lojas

from ...models import Loja

pytestmark = pytest.mark.django_db


@patch('sme_uniforme_apps.proponentes.services.requests.get')
def test_atualiza_coordenadas(mock_get, proponente):
    loja_com_tel_fora_do_formato = Loja(
        proponente=proponente,
        cep='27600-000',
        endereco='Rua Padre Antonio Link',
        bairro='Ferreira',
        numero='113',
        complemento='loja 1',
        telefone='(24) 2452-2568'
    )
    loja_com_tel_fora_do_formato.save()
    mock_get.return_value = Mock(ok=True)
    mock_get.return_value.json.return_value = {
        'features': [
            {'geometry': {'coordinates': ['-47.741721', '-23.595295']}}
        ]
    }
    loja_queryset = Loja.objects.filter(id=loja_com_tel_fora_do_formato.id)
    atualiza_coordenadas_lojas(loja_queryset)
    loja = loja_queryset.get()
    assert [loja.latitude, loja.longitude] == ['-23.595295', '-47.741721']