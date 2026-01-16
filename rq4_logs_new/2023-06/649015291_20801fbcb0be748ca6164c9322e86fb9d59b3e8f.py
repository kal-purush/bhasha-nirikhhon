from behave import given, when, then
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from time import sleep


@given('que estou na página inicial do aplicativo de lista de tarefas')
def step_given_estou_na_pagina_inicial(context):
    context.driver = webdriver.Chrome()
    context.driver.get('https://todomvc.com/examples/vanillajs/')


@when('eu adiciono um novo item com o texto "{texto_item}"')
def step_when_adiciono_novo_item(context, texto_item):
    context.driver.find_element(
        'css selector', '.new-todo').send_keys(texto_item, Keys.ENTER)
    sleep(2)


@when('eu marco o primeiro item como concluído')
def step_when_marco_primeiro_item_concluido(context):
    context.driver.find_element('css selector', '.toggle').click()


@then('o primeiro item na lista deve estar marcado como concluído')
def step_then_primeiro_item_deve_estar_marcado(context):
    primeiro_item = context.driver.find_element(
        'css selector', 'li.completed:nth-child(1)')
    assert primeiro_item.get_attribute('class') == 'completed'