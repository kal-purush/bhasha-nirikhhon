using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TesteFormulas
{
    class GuichesSetup
    {
        public char guiche;
        public Boolean atendente;
        public Boolean vazio;
        public int turnosNecessarios;
        public int guichesIguais;
        //public String nome;

        public int ultimoTurno;
        public Pessoas pessoaDentro;

        public GuichesSetup()
        {
            atendente = false;
            vazio = true;
            ultimoTurno = 1;
            guichesIguais = 1;
        }
    }
}