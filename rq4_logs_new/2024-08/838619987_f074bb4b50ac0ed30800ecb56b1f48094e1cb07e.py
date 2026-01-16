from arbitro import Arbitro
from bot_abstract import BotAbstract
import winsound
import time


class Arena:
    @staticmethod
    def Jugar(bot1: BotAbstract, bot2: BotAbstract):
        puntaje_bot1 = 0
        puntaje_bot2 = 0
        jugada_previa_bot1 = None
        jugada_previa_bot2 = None
        
        # Imprime el encabezado de la tabla
        print("Jugada #", bot1.Nombre, bot2.Nombre, "Ptos. ", "Ptos. ", sep='\t|\t')
        print("-" * 80)

        for i in range(20):
            jugada_bot1 = bot1.Jugar(i, jugada_previa_bot2) 
            jugada_bot2 = bot2.Jugar(i, jugada_previa_bot1) 

            jugada_previa_bot1 = jugada_bot1
            jugada_previa_bot2 = jugada_bot2

            
            puntos_bot1, puntos_bot2 = Arbitro.calcular_puntaje_jugada(jugada_bot1, jugada_bot2)

            puntaje_bot1 += puntos_bot1
            puntaje_bot2 += puntos_bot2 

            # Que suene una nota musical diferente en cada jugada
            frequency = 261.63 * (2 ** (i / 12))
            winsound.Beep(int(frequency), 100)     

            # Imprime el resultado de la jugada
            print(str(i + 1) + '\t', jugada_bot1, jugada_bot2, puntos_bot1, puntos_bot2, sep='\t|\t')
            
            # Espera 0.2 segundos antes de la siguiente jugada
            time.sleep(0.3)

        print("")
        print(bot1.Nombre + ": " + str(puntaje_bot1)) 
        print(bot2.Nombre + ": " + str(puntaje_bot2))
        time.sleep(1)

        return puntaje_bot1, puntaje_bot2