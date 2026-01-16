// Напишите программу, которая принимает на вход координаты точки (X и Y), 
// причём X ≠ 0 и Y ≠ 0 и выдаёт номер координатной четверти плоскости, 
// в которой находится эта точка.

Console.WriteLine("Введите X: ");
int x = int.Parse(Console.ReadLine()!);
Console.WriteLine("Введите Y: ");
int y = int.Parse(Console.ReadLine()!);
if (x > 0 && y > 0)
    Console.WriteLine("I");
else if (x < 0 && y > 0)
    Console.WriteLine("II");
else if (x < 0 && y < 0)
    Console.WriteLine("III");
else if (x > 0 && y < 0)
    Console.WriteLine("IV");
else 
    Console.WriteLine("точка находится на оси");

