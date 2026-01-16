# 111.111.111-11
# 222.222.222-22

# Aleatórios -> 123.654.389-89 876.567.345-95

# CPF válido: 358.302.843-08

# Infos importantes: // -> divisão de inteiros   % -> MOD (resto de divisão)

from unittest import TestCase
import unittest

from validador_cpf import ValidadorCPF


class ValidadorCPFTestCase(TestCase):
    def test_validar_qtd_digitos_cpf(self):
        cpf = "52998224725"

        validador = ValidadorCPF(cpf)

        self.assertTrue(validador.validar_digitos())

    def test_validar_qtd_digitos_cpf_invalido(self):
        cpf = '3583028430'

        validador = ValidadorCPF(cpf)

        self.assertFalse(validador.validar_digitos())



if __name__ == '__main__':
    unittest.main()