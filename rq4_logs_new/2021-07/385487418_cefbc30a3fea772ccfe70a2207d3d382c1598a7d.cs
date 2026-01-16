using System;

namespace Excercise5
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine(" Please, enter a string: "); 
            string keyboard = Console.ReadLine().ToUpper();
            string resultLine = "";
            for (int i = 0; i < keyboard.Length; i++)
            {
                if ((int)keyboard[i] > 64 && (int)keyboard[i] < 91) { 
                   if (keyboard[i] == 'Z') resultLine += "9";
                   else if ((int)keyboard[i] > 82) resultLine += (((int)keyboard[i] - 66) / 3 + 2).ToString();
                   else resultLine += (((int)keyboard[i] - 65) / 3 + 2).ToString();
                }
            }
            Console.WriteLine($" String is converted by mobile to {resultLine}");

            resultLine = "";
            for (int i = 0; i < keyboard.Length; i++)
            {
                switch (keyboard[i])
                {
                    case 'A': case 'B': case 'C': resultLine += "2"; break;
                    case 'D': case 'E': case 'F': resultLine += "3"; break;
                    case 'G': case 'H': case 'I': resultLine += "4"; break;
                    case 'J': case 'K': case 'L': resultLine += "5"; break;
                    case 'M': case 'N': case 'O': resultLine += "6"; break;
                    case 'P': case 'Q': case 'R': case 'S': resultLine += "7"; break;
                    case 'T': case 'U': case 'V': resultLine += "8"; break;
                    case 'W': case 'X': case 'Y': case 'Z': resultLine += "9"; break;
                    default: break;
                }
            }
            Console.WriteLine($" String is converted by mobile to {resultLine}");

                Console.ReadKey();
        }
    }
}