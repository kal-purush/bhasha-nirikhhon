using System;

namespace AsciiFigure
{
    class AsciiFigure
    {
        static void Main(string[] args)
        {
            int classConstant = 7;
            string line = "";
            for (int i = 1; i <= classConstant; i++)
            {
                line = "";
                for (int j = 1; j <= classConstant-i; j++) line += "////";
                for (int j = 1; j < i; j++) line += "********";
                for (int j = 1; j <= classConstant-i; j++) line = line+ "\\\\\\\\";
                Console.WriteLine(line);
            }
            
            
            Console.ReadKey();
        }
    }
}