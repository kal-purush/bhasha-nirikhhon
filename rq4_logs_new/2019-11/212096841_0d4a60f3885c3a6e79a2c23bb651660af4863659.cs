using System;
namespace Tetfuza
{
    public interface IConsole
    {
        public ConsoleKey ReadKey();
        public void Clear();
        public bool KeyAvailable { get; }
    }

    public class InputChecker
    {
        private IConsole _keyboard;

        public InputChecker(IConsole keyboard)
        {
            _keyboard = keyboard;
        }

        /// <summary>
        /// returns true when a key was pressed and the value is waiting in the input buffer.
        /// when true, Console.ReadKey will not block.
        /// </summary>
        /// <returns>
        /// true - a value is in the input buffer
        /// false - no value is available, ReadKey will block
        /// </returns>
        public bool InputAvailable
        {
            get { return _keyboard.KeyAvailable; }
        }

        /// <summary>
        /// returns the rotation, direction, and whether the user pressed the
        /// down key, so we can adjust the pieces on the game board.
        /// </summary>
        /// <param name="direction">returns (-1, 0, or 1 for left, none, and right movement)</param>
        /// <param name="rotation">returns (-1, 0, or 1 for counterclockwise, none, and clockwise rotation)</param>
        /// <param name="down">returns true to move the piece down one space on next frame, False will wait for auto-drop</param>
        /// <returns>
        /// </returns>
        public void GetInput(ref int direction, ref int rotation, ref bool down)
        {
            ConsoleKey key = _keyboard.ReadKey();

            switch (key)
            {
                case ConsoleKey.LeftArrow:
                    direction = -1;
                    break;
                case ConsoleKey.RightArrow:
                    direction = 1;
                    break;
                case ConsoleKey.Z:
                    rotation = -1;
                    break;
                case ConsoleKey.X:
                    rotation = 1;
                    break;
                case ConsoleKey.DownArrow:
                    down = true;
                    break;
                case ConsoleKey.C:
                    _keyboard.Clear();
                    break;
                default:
                    break;
            }
        }
    }
}