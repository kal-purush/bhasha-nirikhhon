using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EstructurasRepetitivas
{
    class Program
    {
        static void Main ( string[] args )
        {
            //int c=21;
            ////c++;//aumenta uno CONTADORES
            ////c--;//disminuye uno
            ////c = c + 1;
            ////c = c - 1;
            ////c += 10;//aumenta en 10 ACUMULADORES
            //c -= 10;//disminuye en 10
            //Console.WriteLine ( "valor c: " + c );
            //Console.ReadLine ();

            /*
             * Estructura repetitiva
             *  while (condicion)
             *  {
             *      --INSTRUCCIONES
             *  }
             */
            //mOSTRAR LOS NUMERO DEL 1 AL 5
            //int i=120;
            //while ( i >= 1 )
            //{
            //    Console.WriteLine ( "numero: " + i );
            //    //i++;
            //    //i += 2;
            //    //i--;
            //    i -= 2;
            //}
            //Console.ReadLine ();
            //1.-Permiir ingresar 10 numero y hallar su suma
            //int i =1;
            //int suma=0;
            //while ( i<=5 )
            //{
            //    Console.WriteLine ( "Ingrese numero " + i );
            //    int numero=int.Parse( Console.ReadLine () );
            //    suma += numero;
            //    i++;
            //}
            //Console.WriteLine ( "Suma :" + suma );
            //Console.ReadLine ();
            //Ingresar un numero y calcular su triple hasta que se ingrese el numero 0
            //string numeroCadeana;
            //int numero=1, numerotriple;
            //while ( numero!=0 )
            //{
            //    Console.WriteLine ( "Ingrese numero" );
            //    numeroCadeana = Console.ReadLine ();
            //    numero = int.Parse ( numeroCadeana );
            //    numerotriple = numero * 3;
            //    Console.WriteLine ( "El triple es " + numerotriple );
            //}
            //Console.WriteLine ( "Finalizó" );
            //Console.ReadLine ();
            //Ingresar un numero e indicar si es par o impar hasta
            //que se ingrese un numero negativo
            //string cadenaNumero;
            //int numero;
            //Console.WriteLine ( "Ingrese numero" );
            //cadenaNumero = Console.ReadLine ();
            //numero = int.Parse ( cadenaNumero );
            //if ( numero % 2 == 0 ) Console.WriteLine ( "Numero Par" );
            //else Console.WriteLine ( "Numnero impar" );
            //while ( numero >=0 )
            //{
            //    Console.WriteLine ( "Ingrese numero" );
            //    cadenaNumero = Console.ReadLine ();
            //    numero = int.Parse ( cadenaNumero );
                
            //    if ( numero < 0 ) break;
                
            //    if ( numero%2 == 0 ) Console.WriteLine ( "Numero Par" );
            //    else Console.WriteLine ( "Numnero impar" );
            //}
            //Console.WriteLine ( "Finalizo" );
            //ingresar numero hasta que el numero negativo
            //while primero analiza y luego ejecuta la instruccion
            //do while, primero lo hace y luego analiza
            //do
            //{
            //    //INSTRUCCION
            //} while ( condicion );
            //int numero, i=1, c=0;
            //string cadenaNumero;
            //do
            //{
            //    Console.WriteLine ( "Ingrese numero :" + i );
            //    cadenaNumero=Console.ReadLine ();
            //    numero = int.Parse ( cadenaNumero );
            //    i++;
            //    c++;
            //} while ( numero >= 0 );
            //Console.WriteLine ( "Fin del programa" );
            //Console.WriteLine ( "Numeros positivos " + (c-1) );
            //Console.ReadLine ();

            //for ( int i = 1; i <= 20; i++ )
            //{
            //    Console.WriteLine ( "Hola: "+i );
            //}
            //Console.ReadLine ();
            //TABLA DE MULTIPLICAR
            //int resultado=0;
            //for ( int i = 1; i <=12; i++ )
            //{
            //    resultado = i * 2;
            //    Console.WriteLine ( "2 x " + i + " = " + resultado);
            //}
            //Console.ReadLine ();

            /*
             * Elaborar un programa que me permita ingresar 2 numero, el primeto tiene que ser menor al segundo. Y dentro de ese rango mostrar los numero pares
             */
            Console.WriteLine ( "Ingrese Rango 1:" );
            int rango1=int.Parse ( Console.ReadLine () );
            Console.WriteLine ( "Ingrese Rango 2:" );
            int rango2=int.Parse ( Console.ReadLine () );
            Console.WriteLine ( "*********************" );
            for ( int i = rango1; i <= rango2; i++ )
            {
                if ( i%2==0 )
                {
                    Console.WriteLine ( i );
                }
            }
            Console.ReadLine ();
        }
    }
}