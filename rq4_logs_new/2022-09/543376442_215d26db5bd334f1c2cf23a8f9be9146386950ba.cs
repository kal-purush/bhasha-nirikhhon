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
 * Elabore un metodo que reciba una cadena de texto como parametro
 * y valide si este cumple con la sucession fibonacci
 */

namespace ProyectoSourceCol.Clases
{
    internal class Punto2
    {
        public void ValidadFibonacci(string Cadena)
        {
            // Evalua si el string es un numero o no, si es numero = true, sino false
            int numeroValue;
            bool isNumber = int.TryParse(Cadena, out numeroValue);
            //Console.WriteLine(isNumber);

            if (isNumber == false)
            {
                Console.WriteLine("No ingreso un numero, ingresaste letras");
            }
            else
            {
                int Numero = Int32.Parse(Cadena); // convierte el string a numero

                if (Numero == 1) //si ingresa el valor 1
                {
                    Console.WriteLine("El numero " + Numero + " Pertenece a la serie fibonacci");
                }
                else
                {
                    int a, b, s;
                    a = 0;
                    b = 1;
                    s = 1;
                    string text = "1";
                    bool isequals = false; //variable vandera que nos dira si aparece el numero en la serie fibonacci
                    while (s <= Numero)// se va a hacer hasta que el numero ingresado sea mayor a s
                    {
                        if (s == Numero)
                        {
                            isequals = true;
                        }
                        s = a + b;
                        text = text + ", " + s;
                        a = b;
                        b = s;
                    }
                    Console.WriteLine(text);//muestra el texto de manera concatenada y organizada para evitar los saltos de liena
                    if (isequals == true)
                    {
                        Console.WriteLine("El numero pertenece a la serie fibonacci" + "\n");
                    }
                    else
                    {
                        Console.WriteLine("El numero no pertenece a la serie fibonacci" + "\n");
                    }
                }
            }
        }
    }
}