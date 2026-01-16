using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lines.ConsoleUI
{
    class GameInfo
    {
        #region Fields

        // Need for two vertical lines, when show rules.
        private ConsoleColor[] _rectColors = new ConsoleColor[6] 
        {
            ConsoleColor.Red,
            ConsoleColor.Blue,
            ConsoleColor.Green,
            ConsoleColor.Yellow,
            ConsoleColor.DarkBlue,
            ConsoleColor.DarkMagenta
        };

        #endregion

        #region Public Methods

        public void ShowGameInfo()
        {
            ShowGameRules();
            int top = 18;
            int left = 10;
            Console.ForegroundColor = ConsoleColor.White;
            Console.BackgroundColor = ConsoleColor.Black;

            Console.SetCursorPosition(left, top);
            Console.WriteLine("Use arrows to control this     rectangle(it's your cursor)");
            DrawCursorExample();
            Console.SetCursorPosition(left, top + 2);
            Console.WriteLine("Press ENTER to select(drag) rectangle");
            Console.SetCursorPosition(left, top + 4);
            Console.WriteLine("Then press ENTER to move(drop) rectangle");
            Console.SetCursorPosition(left, top + 6);
            Console.WriteLine("Your goal is to make lines with lenght >=5");
            Console.SetCursorPosition(left, top + 8);
            Console.WriteLine("Let's go!! Press any key to start .............. Good Luck!");

            #region Draw vetrical lines
            top = 2;
            Console.BackgroundColor = ConsoleColor.Black;
            foreach (var color in _rectColors)
            {
                DrawLineElement(top, color);
                top += 2;
            }
            foreach (var color in _rectColors)
            {
                DrawLineElement(top, color);
                top += 2;
            }
            #endregion
        }

        public void ShowGameRules()
        {
            int top = 2;
            int left = 10;

            Console.ForegroundColor = ConsoleColor.Black;
            Console.BackgroundColor = ConsoleColor.White;
            Console.SetCursorPosition(left, top);
            Console.WriteLine("Rules & Info : ");
            Console.ForegroundColor = ConsoleColor.White;
            Console.BackgroundColor = ConsoleColor.Black;
            Console.SetCursorPosition(left, top + 2);
            Console.WriteLine("It's small rectangle : ");
            DrawSmallRectExample();
            Console.SetCursorPosition(left, top + 4);
            Console.WriteLine("It's big rectangle : ");
            DrawBigRectExample();
            Console.SetCursorPosition(left, top + 6);
            Console.WriteLine("After each move small rectangles increase into big ones");
            Console.SetCursorPosition(left, top + 8);
            Console.WriteLine("If you locate 5 big rectangles in line they disappear");
            Console.SetCursorPosition(left, top + 10);
            Console.WriteLine("PLUS small rectangles doesn't increase");
            Console.SetCursorPosition(left, top + 12);
            Console.WriteLine("You can move rectangle only if path between two cells exist");
            Console.SetCursorPosition(left, top + 14);
            Console.WriteLine("Game ends when all field filled with rectangles");
        }

        public void DrawCursorExample()
        {
            int top = 17;
            int left = 37;
            Console.BackgroundColor = ConsoleColor.White;
            Console.ForegroundColor = ConsoleColor.Black;
            Console.SetCursorPosition(left, top);
            Console.Write('o');
            Console.SetCursorPosition(left + 1, top);
            Console.Write('o');
            Console.SetCursorPosition(left, top + 1);
            Console.Write('o');
            Console.SetCursorPosition(left + 1, top + 1);
            Console.Write('o');
            Console.SetCursorPosition(left + 2, top);
            Console.Write('o');
            Console.SetCursorPosition(left + 2, top + 1);
            Console.Write('o');

            Console.ForegroundColor = ConsoleColor.White;
            Console.BackgroundColor = ConsoleColor.Black;
        }

        public void DrawSmallRectExample()
        {
            int left = 32;
            int top = 3;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.BackgroundColor = Console.ForegroundColor;
            Console.SetCursorPosition(left, top);
            Console.Write('\u2588');
            Console.SetCursorPosition(left + 1, top);

            Console.ForegroundColor = ConsoleColor.White;
            Console.BackgroundColor = ConsoleColor.Black;
        }

        public void DrawBigRectExample()
        {
            int left = 32;
            int top = 5;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.BackgroundColor = Console.ForegroundColor;
            Console.SetCursorPosition(left, top);
            Console.Write('\u2588');
            Console.SetCursorPosition(left + 1, top);
            Console.Write('\u2588');
            Console.SetCursorPosition(left, top + 1);
            Console.Write('\u2588');
            Console.SetCursorPosition(left + 1, top + 1);
            Console.Write('\u2588');
            Console.SetCursorPosition(left + 2, top);
            Console.Write('\u2588');
            Console.SetCursorPosition(left + 2, top + 1);
            Console.Write('\u2588');
            Console.ForegroundColor = ConsoleColor.White;
            Console.BackgroundColor = ConsoleColor.Black;
        }

        public void DrawLineElement(int top, ConsoleColor color)
        {
            int left = 6;
            int right = 70;
            Console.ForegroundColor = color;
            Console.SetCursorPosition(left, top);
            Console.Write('\u2588');
            Console.SetCursorPosition(left + 1, top);
            Console.Write('\u2588');
            Console.SetCursorPosition(left, top + 1);
            Console.Write('\u2588');
            Console.SetCursorPosition(left + 1, top + 1);
            Console.Write('\u2588');
            Console.SetCursorPosition(left + 2, top);
            Console.Write('\u2588');
            Console.SetCursorPosition(left + 2, top + 1);
            Console.Write('\u2588');
            Console.SetCursorPosition(right, top);
            Console.Write('\u2588');
            Console.SetCursorPosition(right + 1, top);
            Console.Write('\u2588');
            Console.SetCursorPosition(right, top + 1);
            Console.Write('\u2588');
            Console.SetCursorPosition(right + 1, top + 1);
            Console.Write('\u2588');
            Console.SetCursorPosition(right + 2, top);
            Console.Write('\u2588');
            Console.SetCursorPosition(right + 2, top + 1);
            Console.Write('\u2588');
        }

        #endregion
    }
}