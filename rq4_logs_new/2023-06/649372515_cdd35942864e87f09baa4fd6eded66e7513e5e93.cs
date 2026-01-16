Console.Write("Enter your number: ");
if (int.TryParse(Console.ReadLine(), out int inptNumber))
{
    var isPrimeNumber = true;
    if (inptNumber > 1)
    {
        for (var cntr = 2; cntr < inptNumber; cntr++)
        {
            if (inptNumber % cntr == 0)
            {
                isPrimeNumber = false;
                break;
            }
        }
    }
    else isPrimeNumber = false;
    var output = isPrimeNumber ? "Prime Number" : "NOT Prime Number";
    Console.WriteLine($"Your number is {output}");
}
else Console.WriteLine("You didn't enter NUMBER");
