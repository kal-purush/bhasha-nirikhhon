#!/usr/bin/env python3
#	./anagrams.py
################################################################################
#      Copyright (C) 2020        Sebastian Francisco Colomar Bauza             #
#      SPDX-License-Identifier:  GPL-2.0-only                                  #
################################################################################

# EXERCISE: 
# Given a list of words from a dictionary file, separate words into classes of words with the same length
# For each class of words of the same length, find all anagrams
# Count the total number of anagrams in each class

# Libraries
import collections

# Declare the variables
fichero='words.txt'

# Clean the list of words
lista=sorted(list(set([palabra.strip().lower() for palabra in open(fichero,'r')])))

# Return a unique value for each word
def unico(palabra):
  return ''.join(sorted(palabra))

# Define an empty dictionary and fill it with each unique value and its associated words
conjunto_unicos=collections.defaultdict(list)
for palabra in lista:
  conjunto_unicos[unico(palabra)].append(palabra)

# Return the anagrams of a word 
def anagramas(palabra):
  return conjunto_unicos[unico(palabra)]

# Return all the anagrams in the list of words
def anagramas_lista(lista):
  return {palabra:anagramas(palabra) for palabra in lista if len(anagramas(palabra))>1}

# Return the count of all the anagrams in the list of words
def anagramas_lista_cuenta(lista):
  return {palabra:len(anagramas(palabra))-1 for palabra in lista if len(anagramas(palabra))>1}

# Return the word with the most number of anagrams in the list
def anagramas_lista_max(lista):
  cuenta_max=0
  for palabra in lista:
    if len(anagramas(palabra))-1>cuenta_max:
      cuenta_max=len(anagramas(palabra))-1
      palabra_max=palabra
  return {palabra_max: cuenta_max}  

# Define an empty dictionary and fill it with each length and its associated words
conjunto_tamano=collections.defaultdict(list)
for palabra in lista:
  conjunto_tamano[len(palabra)].append(palabra)

cuenta_anagramas={}
for tamano,palabras in conjunto_tamano.items():
  cuenta_anagramas[tamano]=sum(1 for palabra in palabras if len(anagramas(palabra))>1)

# Result of the exercise
RESULTADO=cuenta_anagramas
print(RESULTADO)