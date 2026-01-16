#!flask/bin/python
# -*- coding: UTF-8 -*-

from behave import when, then

def soma(x, y):
    """
    Função...
    """
    return x + y


@when('somar "{num_1:d}" com "{num_2:d}"')
def executa_soma(context, num_1, num_2):
    """
    Executa a função de soma e guarda seu resultado no contexto do behave
    """
    context.resultado = soma(num_1, num_2)

@then('o resultado deve ser "{result:d}"')
def assert_resultado(context, result):
    """
    Usa o valor armazenado no contexto e valida se é igual ao resultado
    """
    assert context.resultado == result

main()
