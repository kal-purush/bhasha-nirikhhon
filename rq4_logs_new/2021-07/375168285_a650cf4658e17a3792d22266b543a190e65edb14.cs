using System;

namespace VectoresyMatricesIII
{
    class Program
    {
        static void Main(string[] args)
        {
            /*int[] V1 = { 1,2,3,4,100,54,89,63};
            Array.Sort(V1);
            foreach (int item in V1)
            {
                Console.Write(item+",");
            }
            Console.WriteLine("");*/
            /*for (int i = V1.Length-1; i >= 0; i--)
            {
                Console.Write(V1[i] + ",");
            }*/
            /* Array.Reverse(V1);
             //Metodo de ordenamiento mas sencillo es el de burbuja
             foreach (int item in V1)
             {
                 Console.Write(item + ",");
             }*/
            /*string[] S1 = {"Juan","Ana","Maria","Anastacia","Guillermo","David"};
            Array.Sort(S1);
            foreach (string item in S1)
            {
                Console.Write(item + ",");
            }*/
            //int[] V1 = { 1, 2, 3, 4, 100, 54, 89, 63 };
            /*V1=Array.Empty<int>();*/
            /*Array.Clear(V1,4,3);
            foreach (int item in V1)
            {
                Console.Write(item + ",");
            }*/
            /*int[] V2 = new int[3];
            for (int i = 0; i < V2.Length; i++)
            {
                V2[i] = i;
            }
            Array.Resize<int>(ref V2,V2.Length+5);
            foreach (int item in V2)
            {
                Console.Write(item + ",");
            }*/
            /**
             * Ejemplo : realice un programa en donde ingrese notas de alumnos preguntando al usuario si desea
             * ingresar mas notas S||N y de ser si que pueda continuar, si se agota el espacio ampliarlo*/
            int[] notas = new int[5];
            char opc;
            int contador = 0;
            do
            {
                if (contador<notas.Length) {
                    Console.WriteLine("Ingrese la nota del alumno " + (contador + 1));
                    notas[contador] = int.Parse(Console.ReadLine());
                    contador++;
                }
                else
                {
                    Console.WriteLine("Se amplio el espacio a 2 notas mas");
                    Array.Resize<int>(ref notas, notas.Length + 2);
                }
                Console.WriteLine("Desea ingresar mas notas? S||N");
                opc = char.Parse(Console.ReadLine());
            } while (opc=='S');
            foreach (int item in notas)
            {
                Console.Write(item + ",");
            }

            Console.ReadKey();
        }
    }
}