using System;

namespace ConsoleApp1
{
    class Program
    {
        
        /*Una clase es una plantilla para la creacion de objetos segun un modelo
         predefinido,
        Esta formada por atributos(caracteristicas) y metodos(acciones)
        */
        /*POO:
         1)Abstracion:
        Obtener los atributos y metodos principales de una clase

         2)Encapsulacion
         3)Herencia
         4)Polimorfismo
        */
        static void Main(string[] args)
        {
            int x = 7;
            Console.WriteLine("Hello World!");
            Holamundo();
            Console.WriteLine("El numero sumado es:"+numeroMasUno(x));
            Console.ReadKey();
        }
        /*
         se utiliza static en todos los metodos que sean creados 
        dentro de la clase donde se encuentre el metodo main
         
         */
        public static void Holamundo()
        {
            Console.WriteLine("Hola mundo");
        }
        public static int numeroMasUno(int n)
        {
            return n + 5;
        }
        /*
         Tipos de acceso:
        --------------------------
        -private:
        solo la clase  que lo contiene podra acceder al mismo
        +public:
        todas las clases y metodos podran acceder a el
        #protected:
        solo la clase que lo contiene asi como sus derivadas podran acceder al mismo
        UML=Lenguaje de unificado y modelado
         */
    }
}