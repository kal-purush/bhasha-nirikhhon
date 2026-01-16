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
 * Elabore un metodo que reciba un arreglo de numeros como parametro y devuelva
 * un arreglo de numeros ordenados de mayor a menor y cuente cuantos numeros pares hay
 */

namespace ProyectoSourceCol.Clases
{
    internal class Punto3
    {
        public int[] OrganizarArreglo(int[] arreglo)
        {
            //metodo de ordenamiento donde va remplazando en bien va encontrando el mayor por la pos actual
            for (int i = 0; i < arreglo.Length; i++)
            {
                for (int j = i; j < arreglo.Length - 1; j++)
                {
                    if (arreglo[i] < arreglo[j + 1])
                    {
                        int aux = arreglo[i];
                        arreglo[i] = arreglo[j + 1];
                        arreglo[j + 1] = aux;
                    }
                }
            }
            Console.WriteLine("Arreglo ordenado con exito de mayor a menor");
            return arreglo;
        }

        public int[] MostrarArreglo(int[] arreglo)
        {
            int numpar = 0; //cuenta los numeros pares
            string TextArreglo = "";
            for (int i = 0; i < arreglo.Length; i++)//llena el arreglo con numeros aleatorios entre 1 y 100
            {
                if ((int)arreglo[i] % 2 == 0)
                {
                    numpar++;
                }
                TextArreglo = TextArreglo + " | " + arreglo[i];
            }
            Console.WriteLine("Arreglo: " + TextArreglo + " | " ); //muestra el vector con los numeros aleatorios
            Console.WriteLine("Cantidad de numeros pares: " + numpar + "\n");

            return arreglo;
        }

        public int[] CrearArrelo(int[] arreglo)
        {
            Random myObject = new Random();//genera un numero aleatorio
            string TextArreglo = "";
            for (int i = 0; i < arreglo.Length; i++)//llena el arreglo con numeros aleatorios entre 1 y 100
            {
                arreglo[i] = myObject.Next(1, 100);
                TextArreglo = TextArreglo + " | " + arreglo[i];
            }
            Console.WriteLine("Arreglo: " + TextArreglo + " | "); //muestra el vector con los numeros aleatorios

            return arreglo;
        }
    }
}