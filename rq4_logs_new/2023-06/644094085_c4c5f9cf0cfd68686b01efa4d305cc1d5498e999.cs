
internal class Program
{
    private static void Main(string[] args)
    {
        task25();
        task27();
        task29();
    }

    public static int prompt(String message)
    {
        Console.Write(message);
        return int.Parse(Console.ReadLine()!);
    }

    public static void task25()
    {
        int number = prompt("Введите число: ");
        int power = prompt("Введите степень: ");
        if (power < 1) Console.WriteLine("Степень должна быть больше 0");
        else Console.WriteLine(pow(number, power));
    }

    public static long pow(int number, int power)
    {
        long result = 1;
        for (int i = 1; i <= power; i++)
        {
            result = result * number;
        }
        return result;
    }

    public static void task27()
    {
        int number = prompt("Введите число: ");
        Console.WriteLine(sumDigitInNumber(number));
    }

    public static int sumDigitInNumber(int number)
    {
        int result = 0;
        while (number != 0)
        {
            result = result + number % 10;
            number /= 10;
        }
        return result;
    }

    public static void task29()
    {
        int lengthArray = prompt("Введите длину массива: ");
        printArray(lengthArray);
    }

    public static void printArray(int lengthArray)
    {
        int[] array = new int[lengthArray];
        Random random = new Random();
        for (int i = 0; i < array.Length; i++)
        {
            array[i] = random.Next(0, 100);
        }
        Console.Write("[");
        for (int i = 0; i < array.Length - 1; i++)
        {
            Console.Write(array[i] + ", ");
        }
        Console.Write(array[lengthArray - 1] + "]");
    }
}