Console.WriteLine("************* Menu ****************");
Console.WriteLine("Please choose one this options");

Console.WriteLine("1.Add two numbers");
Console.WriteLine("2.Add list of numbers");

int option = Convert.ToInt16(Console.ReadLine());

if (option == 1)
{
    decimal firstNumber, secondNumber, result;
    Console.Write("Enter first number:");
    firstNumber = Convert.ToDecimal(Console.ReadLine());
    Console.Write("Enter second number:");
    secondNumber = Convert.ToDecimal(Console.ReadLine());

    result = firstNumber + secondNumber;
    Console.WriteLine($"Result is {result}");
}
else if(option == 2)
{
    decimal sum = 0;
    string[] numbers;
    Console.Write("Enter comma-separated (,) integer values:");
    numbers = Console.ReadLine().Split(",");
    foreach (var number in numbers)
    {
        sum += Convert.ToDecimal(number);
    }

    Console.WriteLine($"result is {sum}");
}
else
{
    Console.WriteLine("you entered wrong option");
}