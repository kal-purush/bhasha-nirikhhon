using System;
using System.Collections.Generic;

namespace Colecoes.Helper
{
    public class OperacoesList
    {
        public void ImprimirListString(List<string> lista)
        {
            for (int i = 0; i < lista.Count; i++)
            {
                Console.WriteLine(lista[i]);
            }
        }
    }
}