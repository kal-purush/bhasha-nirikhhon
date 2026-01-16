using System;
using static System.Console;
using System.Collections.Generic;
using System.Linq;



namespace Lesson04_Metodos
{
    class Program
    {
        static void Main(string[] args)
        {

            Title="METODOS EN C#8";

            //1 METODOS VOID: Metodos que realizan una operacion dada
            //pero no retornan ningun dato
            WriteLine("METODOS VOID");

            Saludar(); //Metodo que saluda

            //2.
            WriteLine("\n USANDO TUPLAS");

            string cadena = "Jorge Baltodano,45";
            string cadena1 = "Luisandro Castillejos,20";
            string cadena2 = "Fio,33";

            //Primera forma : Forma manual
            WriteLine(cadena.Substring(0,15));
            WriteLine(cadena.Substring(16)); ;

            //Segunda Forma : Usando IndexOf
            WriteLine("-------------------");
            WriteLine(cadena1.Substring(0, cadena1.IndexOf(","))); // Obtengo nombres
            WriteLine(cadena1.Substring(cadena1.IndexOf(",")+1));

            //Tercera Forma : Implementar Tuplas
            WriteLine("-------------------");
            WriteLine( $"{ObtieneDatos(cadena).name} \n {ObtieneDatos(cadena).age}");
            WriteLine($"{ObtieneDatos(cadena1).name} \n {ObtieneDatos(cadena1).age}");
            WriteLine($"{ObtieneDatos(cadena2).name} \n {ObtieneDatos(cadena2).age}");



            WriteLine("Enter close");
            _= ReadKey();
        }

        public static (string name, string age) ObtieneDatos(string cadena)
        {
            string name = cadena.Substring(0, cadena.IndexOf(","));
            string age = cadena.Substring(cadena.IndexOf(",") + 1);
            return (name, age);
        }

        public static void Saludar() => WriteLine("Hola, soy un metodo void..!");
        public static int DivisionCinco(int numero) => numero / 5;
    }
}