class Calculadora:
    historico = {}
    resultado = ''

    @classmethod
    def mostrar_resultado(cls, current_number):
        current_number.config(text=str(eval(Calculadora.resultado)))
    
    
    @classmethod
    def somar(cls, current_number):
        Calculadora.igual(current_number)
        Calculadora.resultado = current_number['text'] + '+'
        
    
    @classmethod
    def igual(cls, current_number):
        Calculadora.resultado = Calculadora.resultado + current_number['text']
        Calculadora.mostrar_resultado(current_number)
    