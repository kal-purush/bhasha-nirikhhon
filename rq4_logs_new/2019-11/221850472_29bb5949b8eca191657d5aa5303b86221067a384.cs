using System;
using System.Collections.Generic;
using System.Text;

namespace Jaulas
{
    public class Leao : Animal, IFelino
    {
        public Leao(string corDoPelo, string nome)
        {
            this.Pelagem = corDoPelo;
            this.Nome = nome;
            this.DataEntrada = DateTime.Now;
            this.NumeroJaula = new Random().Next(0, 200000);
        }
        public string Pelagem { get ; set ; }

        public string Comunicacao()
        {
            return "Rugido";
        }

        public int TempoVida()
        {
            return 14;
        }

        public string TipoAlimentacao()
        {
            return "Carne";
        }

        public bool Violento()
        {
            return true;

        }
    }
}