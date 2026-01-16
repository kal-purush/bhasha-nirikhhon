class Calculadora:
  """
  Classe que implementa uma calculadora básica com operações de soma.
  """

  def soma(self, num1: int, num2: int) -> int:
    """
    Realiza a soma de dois números inteiros.

    Args:
      num1: O primeiro número inteiro.
      num2: O segundo número inteiro.

    Returns:
      A soma dos dois números.
    """
    return num1 + num2


# Entrada de dados
num1 = int(input())
num2 = int(input())

# Cria uma instância da classe Calculadora
calculadora = Calculadora()

# Realiza a soma e imprime o resultado
resultado = calculadora.soma(num1, num2)
print(resultado)