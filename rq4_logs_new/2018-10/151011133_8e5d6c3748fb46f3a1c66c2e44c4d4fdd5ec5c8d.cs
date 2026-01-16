using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bitmons_Entrega2
{
    class Bitmon
    {
        string nombre;
        int stamina;
        int vida;
        int naturaleza;
        int AttackBase;
        int DefBase;
        public bool estaActivo = false;

        public Bitmon(string nombre, int stamina, int vida, int naturaleza, int attack, int def)
        {
            this.nombre = nombre;
            this.stamina = stamina;
            this.vida = vida;
            this.naturaleza = naturaleza;
            AttackBase = attack;
            DefBase = def;
        }
    }
}