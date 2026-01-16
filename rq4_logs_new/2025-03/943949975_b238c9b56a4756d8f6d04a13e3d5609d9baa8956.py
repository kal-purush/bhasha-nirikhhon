#importacion del modulo uniTest para poder realizar pruebas unitarias
import unittest
#importacion de la clase calculadora desde el archivo de calculadora.py
from calculadora import Calculadora

#definicion de la clase de pruebas que hereda de unittest.TestCase
class TestCalculator(unittest.TestCase):
    #metodo que se ejecuta antes de cada prueba
    def setUp(self):
        self.calc = Calculadora()
    
    #prueba del metodo Suma
    def test_suma(self):
        #prueba de suma de dos numeros positivos
        self.assertEqual(self.calc.suma(2,2), 4)
        #prueba de suma de dos numeros negativos
        self.assertEqual(self.calc.suma(-2,-2), -4)
        #prueba de la suma de dos ceros
        self.assertEqual(self.calc.suma(0,0), 0)

    #prueba del metodo resta
    def test_resta(self):
        #prueba de resta de dos numeros positivos
        self.assertEqual(self.calc.resta(5,3), 2)
        #prueba de resta de dos numeros iguales
        self.assertEqual(self.calc.resta(2,2), 0)

    #prueba del metodo multiplicacion
    def test_multiplicacion(self):
        #prueba de multiplicacion de dos numeros positivos
        self.assertEqual(self.calc.multiplicacion(2,2), 4)
        #prueba de multiplicacion de un numeros negativo por 1
        self.assertEqual(self.calc.multiplicacion(-2,1), -2)
        #prueba de la multiplicacion por cero (debe dar cero)
        self.assertEqual(self.calc.multiplicacion(0,0), 0)
    
    #prueba del metodo divisor
    def test_divisor(self):
        #prueba de la division exacta
        self.assertEqual(self.calc.divisor(4,2), 2)
        #prueba la division con resultado decimal
        self.assertAlmostEqual(self.calc.divisor(3,2), 1.5)
        #prueba con division periodica usando assertAlmostEquals para comparar con precision limitada
        self.assertAlmostEqual(self.calc.divisor(10,3), 3.33333, places=4)

#bloque condicional que permite ejecutar las pruebas directamente
if __name__ == '__main__':
    #inicializar la ejecucion de las pruebas
    unittest.main()