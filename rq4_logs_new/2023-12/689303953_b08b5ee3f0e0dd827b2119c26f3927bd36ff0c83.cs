using System;
using System.IO;

class Program
{
    static char[,] map; // карта игры
    static int playerX, playerY; // координаты игрока
    static int health; // здоровье игрока
    static int attackPower; // сила удара игрока
    static int steps; // количество сделанных шагов
    static int enemyCount; // количество врагов на карте
    static int healthAidCount; // количество аптечек на карте
    static int buffCount; // количество баффов на карте

    static Random random = new Random();

    static void Main()
    {
        InitializeGame();

        while (true)
        {
            DrawMap();
            DrawStats();

            var input = Console.ReadKey();

            if (input.Key == ConsoleKey.UpArrow)
            {
                MovePlayer(0, -1);
            }
            else if (input.Key == ConsoleKey.RightArrow)
            {
                MovePlayer(1, 0);
            }
            else if (input.Key == ConsoleKey.DownArrow)
            {
                MovePlayer(0, 1);
            }
            else if (input.Key == ConsoleKey.LeftArrow)
            {
                MovePlayer(-1, 0);
            }
            else if (input.Key == ConsoleKey.Escape)
            {
                SaveGame();
                Console.WriteLine("\nИгра сохранена.");
                break;
            }

            if (health <= 0)
            {
                Console.WriteLine("\nВы проиграли.");
                break;
            }

            if (enemyCount == 0)
            {
                Console.WriteLine($"\nВы выиграли! Количество шагов: {steps}.");
                break;
            }
        }

        Console.WriteLine("Нажмите любую клавишу для выхода...");
        Console.ReadKey();
    }

    static void InitializeGame()
    {
        map = new char[25, 25];
        playerX = 12;
        playerY = 12;
        health = 50;
        attackPower = 10;
        steps = 0;
        enemyCount = 10;
        healthAidCount = 5;
        buffCount = 5;

        // генерация карты
        for (int i = 0; i < 25; i++)
        {
            for (int j = 0; j < 25; j++)
            {
                map[i, j] = '_';
            }
        }

        // размещение игрока на карте
        map[playerY, playerX] = 'P';

        // размещение врагов на карте
        for (int i = 0; i < enemyCount; i++)
        {
            int enemyX, enemyY;
            do
            {
                enemyX = random.Next(25);
                enemyY = random.Next(25);
            } while (map[enemyY, enemyX] != '_');
            map[enemyY, enemyX] = 'E';
        }

        // размещение аптечек на карте
        for (int i = 0; i < healthAidCount; i++)
        {
            int healthAidX, healthAidY;
            do
            {
                healthAidX = random.Next(25);
                healthAidY = random.Next(25);
            } while (map[healthAidY, healthAidX] != '_');
            map[healthAidY, healthAidX] = 'H';
        }

        // размещение баффов на карте
        for (int i = 0; i < buffCount; i++)
        {
            int buffX, buffY;
            do
            {
                buffX = random.Next(25);
                buffY = random.Next(25);
            } while (map[buffY, buffX] != '_');
            map[buffY, buffX] = 'B';
        }
    }

    static void DrawMap()
    {
        Console.Clear();
        for (int i = 0; i < 25; i++)
        {
            for (int j = 0; j < 25; j++)
            {
                Console.Write(map[i, j]);
            }
            Console.WriteLine();
        }
    }

    static void DrawStats()
    {
        Console.WriteLine($"\nШагов: {steps} | Здоровье: {health} | Сила удара: {attackPower}");
    }

    static void MovePlayer(int offsetX, int offsetY)
    {
        int newX = playerX + offsetX;
        int newY = playerY + offsetY;

        if (newX >= 0 && newX < 25 && newY >= 0 && newY < 25)
        {
            char destination = map[newY, newX];

            if (destination == '_')
            {
                map[playerY, playerX] = '_';
                playerX = newX;
                playerY = newY;
                map[playerY, playerX] = 'P';
                steps++;
            }
            else if (destination == 'E')
            {
                int enemyHP = 30;
                while (enemyHP > 0 && health > 0)
                {
                    enemyHP -= attackPower;
                    health -= 5;
                }

                if (enemyHP <= 0)
                {
                    map[playerY, playerX] = '_';
                    playerX = newX;
                    playerY = newY;
                    map[playerY, playerX] = 'P';
                    steps++;
                    enemyCount--;
                }
            }
            else if (destination == 'H')
            {
                health = 50;
                map[playerY, playerX] = '_';
                playerX = newX;
                playerY = newY;
                map[playerY, playerX] = 'P';
                steps++;
                healthAidCount--;
            }
            else if (destination == 'B')
            {
                attackPower *= 2;
                map[playerY, playerX] = '_';
                playerX = newX;
                playerY = newY;
                map[playerY, playerX] = 'P';
                steps++;
                buffCount--;
            }
        }
    }

    static void SaveGame()
    {
        using (StreamWriter writer = new StreamWriter("save.txt"))
        {
            writer.WriteLine(playerX);
            writer.WriteLine(playerY);
            writer.WriteLine(health);
            writer.WriteLine(attackPower);
            writer.WriteLine(steps);
            writer.WriteLine(enemyCount);
            writer.WriteLine(healthAidCount);
            writer.WriteLine(buffCount);
        }
    }
}