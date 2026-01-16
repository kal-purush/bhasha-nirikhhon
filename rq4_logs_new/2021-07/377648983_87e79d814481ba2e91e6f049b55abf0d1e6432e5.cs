using System;

ExploreIf();
ExploreLoops();
LoopTutorialTest();


/// <summary>
/// This method demonstrates working knowledge of the Branches and Loops tutorial
/// </summary>
void LoopTutorialTest()
{
    int counter = 0;

    for (int index = 1; index <= 20; index++)
    {
        if (index % 3 == 0)
        {
            counter = counter + index;
        }
    }

    Console.WriteLine($"The sum of all numbers between 1 and 20 which is divisible by 3 is {counter}");
    Console.WriteLine($"");
}


/// <summary>
/// This method explores the usage of while, do while, and for loops
/// </summary>
void ExploreLoops()
{
    int counter = 0;

    while (counter < 10)
    {
        Console.WriteLine($"Hello World! The counter is {counter}");
        counter++;
    }
    Console.WriteLine($"");

    counter = 0;

    do
    {
        Console.WriteLine($"Hello World! The counter is {counter}");
        counter++;
    } while (counter < 10);

    for (int index = 0; index < 10; index++)
    {
        Console.WriteLine($"Hello World! The index is {index}");
    }
    Console.WriteLine($"");

    for (int row = 1; row < 11; row++)
    {
        for (char column = 'a'; column < 'k'; column++)
        {
            Console.WriteLine($"The cell is ({row}, {column})");
        }
    }
    Console.WriteLine($"");
}

/// <summary>
/// This method demonstrates the use of the "if" statement
/// </summary>
void ExploreIf()
{
    int a = 5;
    int b = 3;
    int c = 4;

    if (a + b > 10)
    {
        Console.WriteLine("The answer is greater than 10.");
    }
    else
    {
        Console.WriteLine("The answer is not greater than 10");
    }
    Console.WriteLine($"");

    if ((a + b + c > 10) && (a == b))
    {
        Console.WriteLine("The answer is greater than 10");
        Console.WriteLine("And the first number is equal to the second");
    }
    else
    {
        Console.WriteLine("The answer is not greater than 10");
        Console.WriteLine("Or the first number is not equal to the second");
    }
    Console.WriteLine($"");

    if ((a + b + c > 10) || (a == b))
    {
        Console.WriteLine("The answer is greater than 10");
        Console.WriteLine("Or the first number is equal to the second");
    }
    else
    {
        Console.WriteLine("The answer is not greater than 10");
        Console.WriteLine("And the first number is not equal to the second");
    }
    Console.WriteLine($"");
}