using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kalkulacka
{
    /// <summary>
    /// Trieda kalkulacky
    /// </summary>
    class Kalkulacka
    {
        public float a;
        public float b;

        /// <summary>
        /// Spocita 2 hodnoty
        /// </summary>
        /// <returns>vracia sucet hodnot a, b</returns>
        public float Sucet()
        {
            return (a + b);
        }
        /// <summary>
        /// Odpocita b od a
        /// </summary>
        /// <returns>vracia rozdiel hodnot a, b</returns>
        public float Rozdiel()
        {
            return (a - b);
        }
        /// <summary>
        /// Vynasoby 2 hodnoty
        /// </summary>
        /// <returns>vracia nasobok hodnot a, b</returns>
        public float Nasobenie()
        {
            return (a * b);
        }
        /// <summary>
        /// Vydeli hodnotu a hodnotou b
        /// </summary>
        /// <returns>vracia a delene b</returns>
        public float Delenie()
        {
            return (a / b);
        }
    }
}