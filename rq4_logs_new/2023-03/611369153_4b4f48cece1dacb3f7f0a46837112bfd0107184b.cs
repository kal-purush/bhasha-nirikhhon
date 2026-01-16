using System;


namespace Excepciones
{
    internal class Program
    {
        static void Main(string[] args)
        {
            /*Excepciones
             * Sirve para manejar los errores. ej: que se entre un dato que no sea el tipo de la variable.
             * se utiliza el try catch, intenta esto y si algo sale mal muestrame esto.
             */
            
            int edad;
            try
            {
                Console.Write("Ingresa tu edad: ");
                edad = int.Parse(Console.ReadLine());
            }
            catch (Exception ex)
            {

                Console.WriteLine("El error es: {0} ",ex.Message);
            }

            

            Console.ReadKey();

        }
    }
}