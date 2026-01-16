using System;

namespace SpaceGame
{
    public static class ConsoleColorManager
    {
        public static ConsoleColor ResourceColor(string resource)
        {
            // When a planet is initialized, make sure the resource name matches here
            switch (resource)
            {
                case "Gold":
                    return ConsoleColor.DarkYellow;
                case "Hull Material":
                    return ConsoleColor.Blue;
                case "Time":
                    return ConsoleColor.Red;
                case "Fuel":
                    return ConsoleColor.DarkMagenta;
                case "Coins":
                    return ConsoleColor.Yellow;
                default:
                    return ConsoleColor.White;
            }
        }
    }
}