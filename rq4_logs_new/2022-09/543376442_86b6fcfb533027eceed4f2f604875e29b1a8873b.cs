/*
 * !--------- Prueba de SourceCol -----------!
 * Autor: Cristian Rendon Rodriguez
 * Version: 1.0
 * Fecha de inicio: 28/09/2022
 * Fecha de entrega: 1/10/2022
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

/*
 * Elabore un metodo que reciba una cadena de texto como parametro y 
 * devuelva la cadena de texto hasta el numero 20. si la cadena de texto se corta, 
 * adicione "..." al final
 */

namespace ProyectoSourceCol.Clases
{
    internal class Punto1
    {
        public string RecibirCadenaHasta20(string Cadena)
        {
            string CadenaNueva = "";
            char[] ArrayString = Cadena.ToCharArray(); //convierto el string a una arreglo de caracteres
            //Console.WriteLine(ArrayString.Length);
            if (ArrayString.Length > 20) //mira que el tamaño de la cadena sea mayor o igual a 20
            {
                for (int i = 0; i < 20; i++)
                {
                    //Console.WriteLine(i+"  -  "+ ArrayString[i]);
                    CadenaNueva = CadenaNueva + char.ToString(ArrayString[i]);
                }
                CadenaNueva = CadenaNueva + "...";
            }
            else
            {
                CadenaNueva = Cadena;
            }

            return CadenaNueva;
        }

    }
}