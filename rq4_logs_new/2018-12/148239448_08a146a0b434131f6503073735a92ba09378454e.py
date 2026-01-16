#!/usr/bin/env python

# class HillClimbing(object):
#
#     def __init__(self, max_iteracoes, max_iteracoes_sem_melhora):
#         self.max_iteracoes = max_iteracoes
#         self.max_iteracoes_sem_melhora = max_iteracoes_sem_melhora
#
#     def executa(self, problema):
#         estado_atual = problema.estado_inicial
#         i = 0
#         j = 0
#
#         while i < self.max_iteracoes or j < self.max_iteracoes_sem_melhora:
#             print(f"{i:03d} - {estado_atual} - {problema.funcao_objetivo(estado_atual)}")
#             vizinho = problema.funcao_sucessora(estado_atual)
#             custo_atual = problema.funcao_objetivo(estado_atual)
#             custo_vizinho = problema.funcao_objetivo(vizinho)
#             if custo_vizinho < custo_atual:
#                 print(f'achou melhor! atual = {custo_atual}  vizinho {custo_vizinho}')
#                 estado_atual = vizinho
#                 j = 0
#             i += 1
#             j += 1


import copy
from pprint import pprint
from random import random, randint

from problema import Problema
from problema_mochila import ProblemaMochila


class ProblemaRainha(Problema):
    """Encontrar a raiz de uma funcao."""

    def __init__(self):
        self.n = 4

    @property
    def estado_inicial(self):
        """Para este problema, parte de um estado aleatorio."""
        estado = [
            [0, 0, 0, 0],
            [0, 0, 0, 0],
            [0, 0, 0, 0],
            [0, 0, 0, 0],
        ]

        cont = 0
        while cont < self.n:
            i = randint(0, self.n - 1)
            j = randint(0, self.n - 1)
            if estado[i][j] == 0:
                estado[i][j] = 1
                cont += 1

        return estado

    def funcao_objetivo(self, estado):
        """Avalia o custo do estado atual."""
        conflitos = 0

        # conflitos em linhas
        for i in range(self.n):
            soma = 0
            for j in range(self.n):
                soma += estado[i][j]
            if soma > 1:
                conflitos += soma

        # conflitos em colunas
        for i in range(self.n):
            soma = 0
            for j in range(self.n):
                soma += estado[j][i]
            if soma > 1:
                conflitos += soma

        # conflitos na diagonal
        # TODO: verificar se tem conflito na diagonal

        return conflitos

    def funcao_sucessora(self, estado):
        """Gera o estado vizinho"""
        pos_vazia = (0, 0)
        pos_rainha = (0, 0)

        while True:
            i = randint(0, self.n - 1)
            j = randint(0, self.n - 1)
            if estado[i][j] == 0:
                pos_vazia = (i, j)
                break

        while True:
            i = randint(0, self.n - 1)
            j = randint(0, self.n - 1)
            if estado[i][j] == 1:
                pos_rainha = (i, j)
                break

        # copy: permite realizar a copia de objetos
        # deepcopy: realiza a copia recursiva dos objetos e seus atributos internos
        vizinho = copy.deepcopy(estado)

        vizinho[pos_rainha[0]][pos_rainha[1]] = 0
        vizinho[pos_vazia[0]][pos_vazia[1]] = 1

        return vizinho


class ProblemaRaiz(Problema):
    """Encontrar a raiz de uma funcao."""

    @property
    def estado_inicial(self):
        """Para este problema, parte de um estado aleatorio."""
        sinal = 1 if randint(0, 1) == 1 else -1
        inteiro = randint(1, 10)
        return sinal * inteiro * random()

    def funcao_objetivo(self, estado):
        """Avalia o custo do estado atual."""
        x = estado
        return x ** 2 - 2

    def funcao_sucessora(self, estado):
        """Gera o estado vizinho"""
        sinal = 1 if randint(0, 1) == 1 else -1
        return estado + sinal * random()


class HillClimbing1(object):

    def __init__(self, max_iteracoes, max_iteracoes_sem_melhora):
        self.max_iteracoes = max_iteracoes
        self.max_iteracoes_sem_melhora = max_iteracoes_sem_melhora

    def executa(self, problema):
        """Implementacao do hill climbing."""

        # Gera o estado inicial
        estado_atual = problema.estado_inicial

        # Criterios de parada
        # 1. numero maximo de iteracoes
        # 2. numero maximo de iteracoes sem melhora
        # 3. tempo maximo
        # 4. atingiu o objetivo

        # Loop principal
        i = 0  # maximo de iteracoes
        j = 0  # maximo de iteracoes sem melhora

        while i < self.max_iteracoes or j < self.max_iteracoes_sem_melhora:

            # Imprime a solucao
            print(f"{i:03d} - {estado_atual} - {problema.funcao_objetivo(estado_atual)}")

            # Gera um estado vizinho
            vizinho = problema.funcao_sucessora(estado_atual)

            # Verifica se o estado vizinho eh melhor que o atual
            custo_atual = problema.funcao_objetivo(estado_atual)
            custo_vizinho = problema.funcao_objetivo(vizinho)

            if custo_vizinho < custo_atual:
                print(f'achou melhor! atual = {custo_atual}  vizinho {custo_vizinho}')
                estado_atual = vizinho
                j = 0

            i += 1
            j += 1

        # while i < self.max_iteracoes or j < self.max_iteracoes_sem_melhora:
        #
        #     # Imprime a solucao
        #     print(f"{i:03d} - {estado_atual} - {problema.funcao_objetivo(estado_atual)}")
        #
        #     # Gera um estado vizinho
        #     vizinho = problema.funcao_sucessora(estado_atual)
        #
        #     # Verifica se o estado vizinho eh melhor que o atual
        #     custo_atual = problema.funcao_objetivo(estado_atual)
        #     custo_vizinho = problema.funcao_objetivo(vizinho)
        #
        #     if custo_vizinho < custo_atual:
        #         print(f'achou melhor! atual = {custo_atual}  vizinho {custo_vizinho}')
        #         estado_atual = vizinho
        #         j = 0
        #
        #     i += 1
        #     j += 1


# Algoritmo GRASP
# Parametros: alpha, numero_interacoes
# enquanto i <= numero_interacoes faca
# Fase de construcao
# Algoritmo de melhoria
# Atualiza melhor solucao encontrada
# i <- i+1
# fim-do-enquanto

class Grasp(object):
    pass

    #def executa(self, max_iteracoes, ):


def main():
    # Definicao do problema
    # p = ProblemaRaiz()
    #p = ProblemaRainha()

    #hc = HillClimbing(max_iteracoes=100, max_iteracoes_sem_melhora=10)
    #hc.executa(p)

    #problema_mochila = ProblemaMochila()
    #items = problema_mochila.gera_aleatorios()
    pass
