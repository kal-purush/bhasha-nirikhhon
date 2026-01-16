using System;

class Player
{
    static void Main()
    {
        while (true)
        {
            string firstEnemy = Console.ReadLine();
            int distanceToFirstEnemy = int.Parse(Console.ReadLine());
            string secondEnemy = Console.ReadLine();
            int distanceToSecondEnemy = int.Parse(Console.ReadLine());

            if (distanceToFirstEnemy < distanceToSecondEnemy) 
            {
                Console.WriteLine(firstEnemy);
            } else 
            {
                Console.WriteLine(secondEnemy);
            }
        }
    }
}