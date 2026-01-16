using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using static System.Console;

namespace Lesson05_Arrays
{
    class Program
    {
        static void Main(string[] args)
        {
            Title = "ARREGLOS EN CSHARP8";

            //1. ARREGLOS UNIDIMENSIONALES
            //Un arreglo unidimensional permite la insercion de varios datos del mismo
            //tipo de dato, un arreglo siempre empieza en el indice 0

            WriteLine("ARREGLOS UNIDIMENSIONALES");
            WriteLine("---------------------------");

            //Arreglo que almacena solo cadenas 

            string[] nombres = new string[6]; // Se crea el arreglo con 6 posiciones, se cuenta desde 0
            nombres[0] = "Jorge"; //Se agrega el elemento Jorge en la posicion 0 del arreglo
            nombres[1] = "Roberto";
            nombres[2] = "Miguel";
            nombres[3] = "Luis";
            nombres[4] = "Lorenzo";
            nombres[5] = "Esteban";

            //Imprimir los valores del arreglo
            WriteLine("Imprime el valor en indice 0");
            WriteLine($"Nombre: {nombres[0]}"); // Imprime solo el elemento en la posicion 0
            WriteLine();

            WriteLine("foreach impreme todos los valores");
            foreach (string nombre in nombres) // Recorre todo el arreglo con el foreach y los imprime
            {
                WriteLine($"Nombre: {nombre}");
            }

            //2. ARREGLOS COMO PARAMETRO EN METODO
            //Los arreglos permiten pasarse como parametros dentro de metodos 

            WriteLine("\nARREGLO COMO PARAMETRO EN METODOS");
            WriteLine("---------------------------");

            int[] valores = new int[]{10,20,30,40,50 }; // Arreglo de tipo int con 5 indices y adiciona valores

            //Metodo que suma los valores del arreglo y se asigna a una variable su resultado
            int valorSuma = SumarValoresArreglo(valores);

            WriteLine($"Suma Total: {valorSuma}");

            //3. USO DE PARAMS PARA MEJORAR METODOOS
            //Los arreglos permiten pasarse como parametros dentro de metodos 

            WriteLine("\nUSO DE PARAMS PARA MEJORAR METODOOS");
            WriteLine("---------------------------");


            WriteLine("\nValores sin Params");
            int dosValores = MultiplicaDosValores(2,5); //Permite multiplicar solo 2 numeros
            int tresValores = MultiplicarTresValores(12,10,12); //Permite multipiplicar solo 3 numeros
            
            WriteLine($"Mult. Dos Valores: {dosValores}");
            WriteLine($"Mult. Tres Valores: {tresValores}");

            WriteLine("\nValores con Params");
            //Con este metodo podemos multiplicar dos, tres , cuatro y etc de numeros
            int paramsDosValores = ParamsVariosValores(10,4);
            int paramsTresValores = ParamsVariosValores(10,4,5);
            int paramsCincoValores = ParamsVariosValores(10,2,3,4,12);
              
            WriteLine($" Mult. Dos valores: {paramsDosValores}");
            WriteLine($" Mult. Tres Valores: {paramsTresValores}");
            WriteLine($" Mult. Varios Valores: {paramsCincoValores}");

            // 4. ARREGLOS BIDIMENSIOANLES
            //Los arreglos bidimensionales te permiten agregan valores en dos dimensiones 
            // de un solo tipo de dato, donde se manejan en filas y columnas 2 x 2 ,3 x 3
            
            WriteLine("\nARREGLO MULTI-DIMENSIONALES");
            WriteLine("---------------------------");

            //Crear arreglo bidimensional
            string[,] datosPersonales = new string[3,3]; // Contendra 3 filas y 3 columnas

            //Forma 1: Rellenando datos en arreglo 
            datosPersonales[0, 0] = "Jorge"; //fila 0 columna 0 
            datosPersonales[0, 1] = "25";    //  fila 0 columna 1 
            datosPersonales[0, 2] = "1.62"; // d fila 0 columna 2

            datosPersonales[1, 0] = "Luis"; //fila 1 columna 0
            datosPersonales[1, 1] = "27";    // fila 1 columna 1
            datosPersonales[1, 2] = "1.82"; //  fila 1 columna 2

            datosPersonales[2, 0] = "Roberto"; //fila 2 columna 0
            datosPersonales[2, 1] = "28";    // fila 2 columna 1
            datosPersonales[2, 2] = "1.72"; // fila 2 columna 2

            //Forma 2 Rellenar Datos en arreglo
            string[,] datosTrabajadores = new string[,] {
                {"Jorge", "25", "1.62" }, // fila 0 , columna 0,1,2
                {"Luis", "27", "1.82"},   // fila 1 , columna 0,1,2
                {"Roberto", "28", "1.72" }  // fila 2 , columna 0,1,2
            };

            //Metodo para imprimir arreglos

            WriteLine("\n==>Array datosPersonales<==\n");
            ImprimirValoresArreglo(datosPersonales);

            WriteLine("\n==>Array DatosTrabajadores<==\n");
            ImprimirValoresArreglo(datosTrabajadores);


            // 4. ARREGLOS DENTADOS
            //Los arreglos dentados o jagger son arreglos generales que agrupan o guardan mas arreglos

            WriteLine("\nARREGLO DENTADOS");
            WriteLine("---------------------------");


            WriteLine("\nEnter closed");
            _ = ReadLine();

        }

        private static void ImprimirValoresArreglo(string[,] coleccion)
        {

            int contFila = 0;

            for (int fila = 0; fila <= coleccion.Rank ; fila++)
            {
                WriteLine($"-Fila {contFila}");

                for (int columna = 0; columna <= coleccion.GetUpperBound(0); columna++)
                {
                    WriteLine($"  columna {columna}: {coleccion[fila,columna]}");
                }

                contFila++;
            }

            //foreach (string valor in coleccion)
            //{ WriteLine(valor);  }
        }

        private static int ParamsVariosValores(params int[] valores)
        {
            int valorTotal = 1;
            
            foreach (int valor in valores)
            {
                valorTotal *= valor;
            }
            return valorTotal;
        }

        private static int MultiplicarTresValores(int v1, int v2, int v3)
        {
            return (v1 * v2) * v3;
        }

        private static int MultiplicaDosValores(int v1, int v2)
        {
            return v1 * v2;
        }

        private static int SumarValoresArreglo(int[] valores)
        {
            int sumaTotal = 0;

            foreach (int valor in valores)
            {
                sumaTotal += valor;
            }

            return sumaTotal;
            
        }
    }
}