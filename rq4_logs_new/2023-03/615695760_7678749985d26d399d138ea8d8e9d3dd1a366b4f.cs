using System.Linq.Expressions;
using System.Net.Http.Headers;
using System.Numerics;

public class Program
{
    public static void Main()
    {
        int n = 0;
        Console.WriteLine("Напишите размер массива");
        try
        {
            n = ReadInt();
            int i = 0;
            int max = 0;
            int secondMax = 0;
            int[] numbers = new int[n];
            while (i < n)
            {
                Console.WriteLine("Введите элемент массива");
                numbers[i] = ReadInt();
                i++;
            }
        
            foreach (int num in numbers)
            {
                if (num > max)
                {
                    max = num;
                }

            }
            Console.WriteLine($"Наибольший элемент = {max}");
            foreach (int num in numbers)
            {
                if (num > secondMax && num < max)
                {
                    secondMax = num;
                }
            }
            Console.WriteLine($"Второй наибольший элемент = {secondMax}");
            for (i = 0; i < n; i++)
            {
                Console.WriteLine($"Элемент {i} :{numbers[i]}");
            }
        }
        catch (System.FormatException)
        {
            Console.WriteLine("Введите цифры!");
        }
     }
       
   
 public static int ReadInt()
    {
        var text = Console.ReadLine();
        if (string.IsNullOrEmpty(text))
        {
            Console.WriteLine($"Введите что-то!");
        }
        return Int32.Parse(text);
    }
   
   
   
}

