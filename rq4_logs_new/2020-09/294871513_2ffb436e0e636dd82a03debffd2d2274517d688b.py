#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 7 09:39:58 2020

@author: Gabriel
"""

from Crypto.Signature import DSS
from Crypto.PublicKey import DSA
from Crypto.Hash import SHA256

####################################################################################################################################################################################################
# Gerador das chaves
###################################################################################################################################################

key = DSA.generate(2048)
publickey = key.publickey().export_key()
print(publickey)

###################################################################################################################################################
# Assinatura da Mensagem
###################################################################################################################################################

msg = input("Qual a mensagem a ser assinada?")
# msg recebe a mensagem a ser assinada
msghash = SHA256.new(msg)
# msghash recebe o SHA256 da mensagem a ser assinada
assinador = DSS.new(key, 'fips-186-3')
# assinador recebe a função de assinatura de Digital Signature Standard 
# com os parâmetro key (chave privada) e 'fips-186-3', que torna a geração 
# assinatura randômica
msgassinada = assinador.sign(msghash)
# msgassinada recebe a mensagem assinada, ou seja,
# o output de 'assinador' quando o imput é o hash da mensagem

###################################################################################################################################################
# Verificador de Assinatura
###################################################################################################################################################


verificador = DSS.new(publickey, 'fips-186-3')
# verificador recebe a função de verificação de Digital Signature Standard
# com os parâmetros publickey (a chave pública) e 'fips-186-3', que reconhece 
# que a assinatura é randomizada
try:
    verificador.verify(msghash, msgassinada)
    print("Essa mensagem é verdadeira")
    # o verificador verifica se a mensagem assinada é autêntica a partir de sua
    # assinatura e de seu hash. Caso positivo, printa isso no console
except:
    print("Essa mensagem é falsa")
    # Caso negativo, printa isso no console




   
   
   