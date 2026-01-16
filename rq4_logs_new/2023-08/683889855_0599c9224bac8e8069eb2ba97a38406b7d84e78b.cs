using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

internal class Shop
{
    public void EnterShop()
    {
        Console.Clear();
        DrawShop(0, 0);
        DrawGoldUI(75, 0);
        DrawShopUI(75, 5);
        Choice();
    }

    public void Choice()
    {
        bool isSelect = false;
        Console.SetCursorPosition(75, 25);
        Console.Write("                          ");
        Console.SetCursorPosition(75, 25);
        Console.Write("선택지를 입력해주세요.: ");

        while (isSelect == false)
        {
            while (isSelect == false)
            {
                string key = Console.ReadLine();

                if (key == "0")
                {
                    Program.entrance.EntranceUI();
                    isSelect = true;
                }
                else if (key == "1")
                {
                    Program.entrance.EntranceUI();
                    isSelect = true;
                }
                else if (key == "2")
                {
                    Program.entrance.EntranceUI();
                    isSelect = true;
                }
                else
                {
                    Console.SetCursorPosition(75, 25);
                    Console.Write("                             ");
                    Console.SetCursorPosition(75, 25);
                    Console.Write("올바른 값을 입력해주세요.: ");
                }
            }
        }
    }

    public void DrawGoldUI(int x, int y)
    {
        Console.SetCursorPosition(x, y);
        Console.Write("*-------------------------------------*");
        Console.SetCursorPosition(x, y + 4);
        Console.Write("*-------------------------------------*");
        for (int i = 1; i < 4; i++)
        {
            Console.SetCursorPosition(x, y+i);
            Console.Write("|");
            Console.SetCursorPosition(x+38, y+i);
            Console.Write("|");
        }
        GameManager.GM.DrawText(x + 2, y + 2, $"현재 소지금은 {Program.player.Gold}골드입니다.", "yellow");
    }

    public void DrawShopUI(int x, int y)
    {
        Console.SetCursorPosition(x, y);
        Console.Write("*-------------------------------------*");
        Console.SetCursorPosition(x, y + 18);
        Console.Write("*-------------------------------------*");
        for (int i = 1; i < 18; i++)
        {
            Console.SetCursorPosition(x, y + i);
            Console.Write("|");
            Console.SetCursorPosition(x + 38, y + i);
            Console.Write("|");
        }
    }

    public void DrawShop(int x, int y)
    {
        Console.SetCursorPosition(x, y + 0);
        Console.Write("                    _____  _   _  _____ ______ ");
        Console.SetCursorPosition(x, y + 1);
        Console.Write("                   /  ___|| | | ||  _  || ___ @");
        Console.SetCursorPosition(x, y + 2);
        Console.Write("                   @ `--. | |_| || | | || |_/ /");
        Console.SetCursorPosition(x, y + 3);
        Console.Write("                    `--. @|  _  || | | ||  __/ ");
        Console.SetCursorPosition(x, y + 4);
        Console.Write("                   /@__/ /| | | |@ @_/ /| | || ");
        Console.SetCursorPosition(x, y + 5);
        Console.Write("                   @____/ @_| |_/ @___/ @_| ||");
        Console.SetCursorPosition(x, y + 6);
        Console.Write("                   ||    Fishing   shop     ||");
        Console.SetCursorPosition(x, y + 7);
        Console.Write("   ________________||_______________________||_____________");
        Console.SetCursorPosition(x, y + 8);
        Console.Write("  |_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_||");
        Console.SetCursorPosition(x, y + 9);
        Console.Write("  |_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|| /|");
        Console.SetCursorPosition(x, y + 10);
        Console.Write("  |_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_||/||");
        Console.SetCursorPosition(x, y + 11);
        Console.Write("  |_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|||/|");
        Console.SetCursorPosition(x, y + 12);
        Console.Write("  |_|_|_|_|_|_|_|_|_|     _      _     |_|_|_|_|_|_|_|_|_|_|/||");
        Console.SetCursorPosition(x, y + 13);
        Console.Write("  |_|               |    (_)    (_)    |      __         |_|/||");
        Console.SetCursorPosition(x, y + 14);
        Console.Write("  |_|.              |__________________|     (^^)  .     |_||/|");
        Console.SetCursorPosition(x, y + 15);
        Console.Write("  |_|*`.            |_|      ||      |_|     _)(_  I`.   |_|/||");
        Console.SetCursorPosition(x, y + 16);
        Console.Write("  |_| S `.          |_|      ||      |_|    /    @/| ;   |_||/|");
        Console.SetCursorPosition(x, y + 17);
        Console.Write("  |_|`. A `.        |_|      ||      |_|   // )_@@/      |_|/||");
        Console.SetCursorPosition(x, y + 18);
        Console.Write("  |_|  `. L `.      |_|     [||]     |_|   @|.//_)       |_||/|");
        Console.SetCursorPosition(x, y + 19);
        Console.Write("  |_|    `. E `.    |_|      ||      |_|     // /        |_|/||");
        Console.SetCursorPosition(x, y + 20);
        Console.Write("  |_|______`__*_`___|_|      ||      |_|_____@@|_________|_||/|");
        Console.SetCursorPosition(x, y + 21);
        Console.Write("  |_|_|_|_|_|_|_|_|_|_|______||______|_|_|_|_|_|_|_|_|_|_|_|/||");
        Console.SetCursorPosition(x, y + 22);
        Console.Write("__|_|_|_|_|_|_|_|_|_|_|______||______|_|_|_|_|_|_|_|_|_|_|_||/________");
        Console.SetCursorPosition(x, y + 23);
        Console.Write(" /     /     /     /     /     /     /     /     /     /     /     /");
        Console.SetCursorPosition(x, y + 24);
        Console.Write("/_____/_____/_____/_____/_____/_____/_____/_____/_____/_____/_____/");
        Console.SetCursorPosition(x, y + 25);
        Console.Write("_______________________________________________________________");
    }
}