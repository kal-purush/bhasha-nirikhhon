/*Напишите программу, которая на вход принимает число (N), 
а на выходе показывает все чётные числа от 1 до N.*/

Console.WriteLine ("Insert number: ");
int number = Convert.ToInt32(Console.ReadLine());
int startnumber = 1;
for (int i = startnumber; i <= number; i++)
{
 if (i % 2 == 0) 
{
Console.Write(i + " ");
}
}