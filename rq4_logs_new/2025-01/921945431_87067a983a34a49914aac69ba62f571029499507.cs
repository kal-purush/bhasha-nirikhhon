
using System.IO.Compression;

namespace Algorithms.Examples;
public static class AlgorithmsConsole
{
    public static void WriteFibonacciNumbers(int maxNumber)
    {
        Console.WriteLine("Fibonacci Numbers");
        Console.WriteLine("---");
        for(int a = 0, b = 1; a <= maxNumber; (a, b) = (b, a + b))
        {
            if(a == 1 && b == 1)
            continue;
            Console.WriteLine(a);    
        }
        Console.WriteLine("");
    }
} 