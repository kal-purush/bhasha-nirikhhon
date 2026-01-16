using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Animais
{
    class Cachorro : Mamifero
    {
        public void enterrarOsso()
        {
            Console.WriteLine("Enterra o osso");
        }
        public void abanarRabo()
        {
            Console.WriteLine("Abana o rabo");
        }
    }
}