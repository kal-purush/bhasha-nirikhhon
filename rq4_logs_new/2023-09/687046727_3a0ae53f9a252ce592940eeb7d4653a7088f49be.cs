
class SuperAdventure
{
    static int playerX = 3; // breedte
    static int playerY = 2; // lengte

    // 2D array
    static char[,] map = {
        {' ', ' ', ' ', '#', ' ', ' ', ' ',' '},
        {' ', ' ', '#', 'P', '#', ' ', ' ',' '},
        {' ', '#', '#', 'A', '#', '#', '#',' '},
        {'#', 'V', 'F', 'T', 'G', 'B', 'S','#'},
        {' ', '#', '#', 'H', '#', '#', '#',' '},
        {' ', ' ', ' ', '#', ' ', ' ', ' ',' '},
    };

    static void DrawMap()
    {
        // wist de terminal (refresht de terminal)
        Console.Clear();
        // de .GetLength(0)  
        // word gebruikt om de totale number van elementen te vinden in de specifieke dimensie van de array
        for (int y = 0; y < map.GetLength(0); y++)
        {
            // hiet word het zelfde gedaan
            for (int x = 0; x < map.GetLength(1); x++)
            {
                // als x en player x en ook bij y en playery gelijk zijn word er een 1 op de map geplaatst
                if (x == playerX && y == playerY)
                    Console.Write('1');
                // map word gevuld met de array
                else
                    Console.Write(map[y, x]);
            }
            Console.WriteLine();
        }
    }

    static void Main()
    {
        while (true)
        {
            // functie begint te werken
            DrawMap();

            // hier worden locatie events getriggerd
            if (map[playerY, playerX] == 'S')
            {
                Console.WriteLine("spider forrest.");
                Console.ReadLine();
            }
            else if (map[playerY, playerX] == 'B')
            {
                Console.WriteLine("Bridge");
            }
            else if (map[playerY, playerX] == 'T')
            {
                Console.WriteLine("Towns square");
            }



            // console van het game hier mee kan je naar boven beneden links en naar rechts
            ConsoleKeyInfo keyInfo = Console.ReadKey();
            ConsoleKey key = keyInfo.Key;

            switch (key)
            {
                // neemt waardes van de X as min 1 af als het niet tegen de muur (#) aankomen
                case ConsoleKey.A:
                    if (playerX > 0 && map[playerY, playerX - 1] != '#')
                        playerX--;
                    break;
                // neemt waardes van de Y as plus 1 bij als het niet tegen de muur (#) aankomen    
                case ConsoleKey.D:
                    if (playerX < map.GetLength(1) - 1 && map[playerY, playerX + 1] != '#')
                        playerX++;
                    break;
                case ConsoleKey.W:
                    if (playerY > 0 && map[playerY - 1, playerX] != '#')
                        playerY--;
                    break;
                case ConsoleKey.S:
                    if (playerY < map.GetLength(0) - 1 && map[playerY + 1, playerX] != '#')
                        playerY++;
                    break;
                // game stopt bij escape
                case ConsoleKey.Escape:
                    return;
            }
        }
    }
}