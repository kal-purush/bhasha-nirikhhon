using System;
using System.Globalization;

namespace EstruturaRepetitiva
{
    public static class DoWhile
    {
        public static void Executar()
        {
            char repetir;

            do
            {
                Console.Write("Informe uma temperatura em CELSIUS: ");
                var celsius = Convert.ToDouble(Console.ReadLine(), CultureInfo.InvariantCulture);

                var fahrenheit = 9.0 * celsius / 5.0 + 32.0;  
                Console.WriteLine("Equivalente a Fahrenheit: {0}", fahrenheit.ToString("F1", CultureInfo.InvariantCulture));

                Console.WriteLine("Deseja repetir (s/n)?");
                repetir = Convert.ToChar(Console.ReadLine());

            } while(repetir == 's'); 

            Console.ReadKey();
        }
    }
}