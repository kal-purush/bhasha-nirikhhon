using System;

namespace CMP1903M_Assessment_1.Debugging
{
    static class Debug
    {
        public const ConsoleColor ErrorColour = ConsoleColor.Red;

        /// <summary>
        /// Writes a normal message to the console using default colours
        /// </summary>
        /// <param name="text">Text to write to the console</param>
        public static void Log(string text)
        {
            Console.ResetColor();
            Console.WriteLine(text);
        }

        /// <summary>
        /// Writes a spooky scary error message to the console in RED!
        /// </summary>
        /// <param name="text">Text to write to the console</param>
        public static void LogError(string text)
        {
            Console.ForegroundColor = ErrorColour;
            Console.WriteLine(text);
            Console.ResetColor();
        }
    }
}