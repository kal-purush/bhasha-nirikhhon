using System;

namespace CozaLozaWoza
{
    class Program
    {
        static void Main(string[] args)
        {
            const int numbersPerLine = 11;
            const int lowerBound = 1;
            const int higherBound = 111;

            int currentIndex = lowerBound-1;
            while (currentIndex<higherBound+1)
            {
                String numberSetLine = "";
                for (int i = 1; i <= numbersPerLine; i++)
                {
                    currentIndex++;
                    String currentLine = "";
                    if (currentIndex > higherBound) break;
                    if (currentIndex % 3 == 0) currentLine += "Coza";
                    if (currentIndex % 5 == 0) currentLine += "Loza";
                    if (currentIndex % 7 == 0) currentLine += "Woza";
                    if (currentLine.Length == 0) currentLine += currentIndex.ToString();
                    numberSetLine += currentLine + " ";


                }
                Console.WriteLine(numberSetLine);
            }


            Console.ReadKey();
        }
    }
}