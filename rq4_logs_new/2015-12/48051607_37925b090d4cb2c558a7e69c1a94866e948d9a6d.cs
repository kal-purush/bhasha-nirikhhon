using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CalculoEstadisiticas
{
    static class InputValidator
    {
        static public int GetNumeric(string readLine)
        {
            int number;
            bool isNumeric = int.TryParse(readLine,out number);
            while(!isNumeric || number < 0)
            {
                Console.WriteLine("Introduce un numero correcto");
                readLine = Console.ReadLine();
                isNumeric = int.TryParse(readLine, out number);
            }
            return number;
        }
    }
}