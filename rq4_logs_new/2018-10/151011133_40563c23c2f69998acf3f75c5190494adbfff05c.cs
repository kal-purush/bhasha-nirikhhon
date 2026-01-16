using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bitmons_Entrega2
{
    class Jugador
    {
        string nombre;
        List<Bitmon> equipo;
        int jugada = 0;

        public Jugador(string nombre)
        {
            this.nombre = nombre;
        }

        public void agregarBitmon(Bitmon bitmon)
        {
            equipo.Add(bitmon);
        }

        private void verEquipo()
        {
            for (int i = 1; i < 4; i++)
            {
                Console.WriteLine("Bitmon {0} ");
            }
        }

        private void verRibal()
        {

        }

        private void verBitmons()
        {

        }

        public void Estrategia(int e)
        {

        }
    }
}