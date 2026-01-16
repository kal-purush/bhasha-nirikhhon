namespace Operators;

class Program
{
    static void Main(string[] args)
    {
        int number1 = 5;
        int number2 = 3;
        int number3;

        // unary operator
        number3 = -number1;
        Console.WriteLine("number3 is {0}", number3);

        bool isSunny = true;
        Console.WriteLine("\n!isSunny is {0}, in fact isSunny  is {1}", !isSunny, isSunny);

        //increment operators
        int number = 0;

        number++;
        Console.WriteLine("\nnumber is {0}", number);
        Console.WriteLine("number++ is {0}", number++);
        // pre increment
        Console.WriteLine("++number is {0}", ++number);

        //decrement operators
        number--;
        Console.WriteLine("\nnumber is {0}", number);
        Console.WriteLine("number-- is {0}", number--);
        // pre decrement
        Console.WriteLine("--number is {0}", --number);

        int result;

        result = number1 + number2;
        Console.WriteLine("\nresult of number1 + number2 is {0}", result);
        result = number1 - number2;
        Console.WriteLine("result of number1 - number2 is {0}", result);
        result = number1 * number2;
        Console.WriteLine("result of number1 * number2 is {0}", result);
        result = number1 / number2;
        Console.WriteLine("result of number1 / number2 is {0}", result);
        result = number1 % number2;
        Console.WriteLine("result of number1 % number2 is {0} - (modulo, as reminder)", result);

        // relational and type operators
        bool isLower;
        isLower = number1 > number2;
        Console.WriteLine("\nresult of number1 > number2 is {0}", isLower);

        bool isEqual;
        isEqual = number1 == number2;
        Console.WriteLine("\nresult of number1 == number2 is {0}", isEqual);
        isEqual = number1 != number2;
        Console.WriteLine("result of number1 != number2 is {0}", isEqual);

        // conditional operators
        bool isLowerAndSunny;
        isLowerAndSunny = isLower && isSunny;
        Console.WriteLine("\nresult of isLower && isSunny is {0}", isLowerAndSunny);

        isLowerAndSunny = isLower || isSunny;
        Console.WriteLine("result of isLower || isSunny is {0}", isLowerAndSunny);

    }
}