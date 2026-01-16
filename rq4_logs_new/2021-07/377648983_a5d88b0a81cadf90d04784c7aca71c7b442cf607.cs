using System;
using System.Collections.Generic;

WorkWithStrings();
WorkWithInts();
ChallangeMethod();

/// <summary>
/// This is a challange method using an int list to display the 20th fibonacci number
/// </summary>
void ChallangeMethod()
{
    var fibonacciChallange = new List<int> { 1, 1 };

    var indexlimit = 20;

    indexlimit = indexlimit - fibonacciChallange.Count;

    for (int index = 1; index <= indexlimit; index++)
    {
        var previous = fibonacciChallange[fibonacciChallange.Count - 1];
        var previous2 = fibonacciChallange[fibonacciChallange.Count - 2];

        fibonacciChallange.Add(previous + previous2);
    }

    foreach (var itm in fibonacciChallange)
    {
        Console.WriteLine(itm);
    }

    Console.WriteLine("");

    Console.WriteLine($"The 20th fibonacci number is {fibonacciChallange[fibonacciChallange.Count - 1]}");
}

/// <summary>
/// This method explores the use of int in lists
/// </summary>
void WorkWithInts()
{
    var fibonacciNumbers = new List<int> { 1, 1 };

    var previous = fibonacciNumbers[fibonacciNumbers.Count - 1];
    var previous2 = fibonacciNumbers[fibonacciNumbers.Count - 2];

    fibonacciNumbers.Add(previous + previous2);

    foreach (var itm in fibonacciNumbers)
    {
        Console.WriteLine(itm);
    }

    Console.WriteLine("");
}

/// <summary>
/// This method demonstrates working with strings in a list
/// </summary>
void WorkWithStrings()
{
    var names = new List<string> { "David", "Ana", "Felipe" };

    foreach (var name in names)
    {
        Console.WriteLine($"Hello {name.ToUpper()}!");
    }

    Console.WriteLine();

    names.Add("Maria");
    names.Add("Bill");
    names.Remove("Ana");

    foreach (var name in names)
    {
        Console.WriteLine($"Hello {name.ToUpper()}!");
    }

    Console.WriteLine();

    Console.WriteLine($"My name is {names[0]}");
    Console.WriteLine($"I've added {names[2]} and {names[3]} to the list");
    Console.WriteLine($"The list has {names.Count} people in it");

    Console.WriteLine();

    var index = names.IndexOf("Felipe");

    if (index == -1)
    {
        Console.WriteLine($"When an item is not found, IndexOf returns {index}");
    }
    else
    {
        Console.WriteLine($"The name {names[index]} is at index {index}");
    }

    index = names.IndexOf("Not Found");

    if (index == -1)
    {
        Console.WriteLine($"When an item is not found, IndexOf returns {index}");
    }
    else
    {
        Console.WriteLine($"The name {names[index]} is at index {index}");
    }

    Console.WriteLine("");

    names.Sort();

    foreach (var name in names)
    {
        Console.WriteLine($"Hello {name.ToUpper()}!");
    }

    Console.WriteLine("");
}