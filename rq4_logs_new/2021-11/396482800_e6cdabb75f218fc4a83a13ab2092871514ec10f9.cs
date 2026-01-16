using System;

using HomeworkMargaret.Tools;

namespace HomeworkMargaret.General
{
    class DA_Game
    {
        public static void Start()
        {
            char[,] map = new char[10, 10];
            MapPreparation(map);

            bool mouseCatchCheese = false;
            do
            {                
                ShowMap(map);
                ConsoleKeyInfo keyInfo = Console.ReadKey();
                             
                switch (keyInfo.Key)
                {
                    case ConsoleKey.UpArrow:
                        MoveUp(map);
                        break;

                    case ConsoleKey.DownArrow:
                        MoveDown(map);
                        break;

                    case ConsoleKey.LeftArrow:
                        MoveLeft(map);
                        break;

                    case ConsoleKey.RightArrow:
                        MoveRight(map);
                        break;

                    default:
                        break;
                }

                Console.Clear();

                mouseCatchCheese = IsCheeseCaught(map);
            } while (mouseCatchCheese != true);
            YouWin();
        }

        public static void MapPreparation(char[,] map)
        {

            int horizontalSize = map.GetLength(1);
            int verticalSize = map.GetLength(0);
            for (int lineIndx = 0; lineIndx < map.GetLength(0); lineIndx++)
            {
                for(int rowIndx = 0; rowIndx < map.GetLength(1); rowIndx++)
                {
                    if ((lineIndx == 0 || lineIndx == horizontalSize - 1) || (rowIndx == 0 || rowIndx == verticalSize - 1))
                    {
                        map[lineIndx, rowIndx] = '*';
                    }
                    else
                    {
                        map[lineIndx, rowIndx] = ' ';
                    }
                }
            }

            Random rand = new Random();
            for (int i = 0; i < 20; i++)
            {
                int vert = rand.Next(2, horizontalSize - 2);
                int horiz = rand.Next(2, verticalSize - 2);
                map[vert, horiz] = '*';
            }

            int mVerticalCoord = rand.Next(1, horizontalSize - 2);
            int mHorizontalCood = rand.Next(1, verticalSize - 2);            
            map[mVerticalCoord, mHorizontalCood] = 'M';            
            
            int сVerticalCoord = rand.Next(1, horizontalSize - 2);
            int сHorizontalCood = rand.Next(1, verticalSize - 2);
            map[сVerticalCoord, сHorizontalCood] = 'C';
        }

        public static void ShowMap(char[,] array)
        { 
            for (int i = 0; i < array.GetLength(0); i++)
            {
                for (int j = 0; j < array.GetLength(1); j++)
                {
                    if (array[i, j] == 'M')
                    {
                        Console.ForegroundColor = ConsoleColor.White;
                        CT.Print($"{array[i, j]} ", false);
                        Console.ForegroundColor = ProgramMargaret.AppForegroundColor;
                    }
                    else if (array[i, j] == 'C')
                    {
                        Console.ForegroundColor = ConsoleColor.DarkYellow;
                        CT.Print($"{array[i, j]} ", false);
                        Console.ForegroundColor = ProgramMargaret.AppForegroundColor;
                    }
                    else
                    {
                        CT.Print($"{array[i, j]} ", false);
                    }
                }
                CT.Print("");
            }
        }

        public static void GetMouseCoordinates(char[,] map, out int mouseH, out int mouseW)
        {
            mouseH = -1;
            mouseW = -1;

            for (int i = 0; i < map.GetLength(0); i++)
            {
                for (int j = 0; j < map.GetLength(1); j++)
                {
                    if (map[i, j] == 'M')
                    {
                        mouseH = i;
                        mouseW = j;
                    }
                }
            }
        }

        public static void MoveUp(char[,] map)
        {
            // 1. найти текущие координаты мыши
            // 2. проверить, можно ли сделать шаг вверх
            // 3. сделать шаг 
            
            GetMouseCoordinates(map, out int mouseH, out int mouseW);

            int stepH = mouseH - 1;
            int stepW = mouseW;

            if(map[stepH,stepW] != '*')
            {
                map[mouseH, mouseW] = ' ';
                map[stepH, stepW] = 'M';
            }            
        }

        public static void MoveDown(char[,] map)
        {
            GetMouseCoordinates(map, out int mouseH, out int mouseW);

            int stepH = mouseH + 1;
            int stepW = mouseW;

            if (map[stepH, stepW] != '*')
            {
                map[mouseH, mouseW] = ' ';
                map[stepH, stepW] = 'M';
            }
        }

        public static void MoveLeft(char[,] map)
        {
            GetMouseCoordinates(map, out int mouseH, out int mouseW);

            int stepH = mouseH;
            int stepW = mouseW - 1;

            if (map[stepH, stepW] != '*')
            {
                map[mouseH, mouseW] = ' ';
                map[stepH, stepW] = 'M';
            }
        }

        public static void MoveRight(char[,] map)
        {
            GetMouseCoordinates(map, out int mouseH, out int mouseW);

            int stepH = mouseH;
            int stepW = mouseW + 1;

            if (map[stepH, stepW] != '*')
            {
                map[mouseH, mouseW] = ' ';
                map[stepH, stepW] = 'M';
            }
        }

        public static bool IsCheeseCaught(char[,] map)
        {
            for (int i = 0; i < map.GetLength(0); i++)
            {
                for (int j = 0; j < map.GetLength(1); j++)
                {
                    if (map[i, j] == 'C')
                    {
                        return false;                   
                    }                   
                }
            }
            return true;
        }

        public static void YouWin()
        {
            Console.ForegroundColor = ConsoleColor.Magenta;            
            CT.Print("**************************");
            CT.Print("*                        *");
            CT.Print("*         YOU            *");
            CT.Print("*         WIN            *");
            CT.Print("*                        *");
            CT.Print("**************************");
            Console.ForegroundColor = ProgramMargaret.AppForegroundColor;
        }
    }
}