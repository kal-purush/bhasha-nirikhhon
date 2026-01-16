namespace System
{
    public static class Cli
    {
        public static void WriteLine(
            string text = "",
            ConsoleColor? foregroundColor = null,
            ConsoleColor? backgroundColor = null)
        {
            WriteToConsoleWith(Console.WriteLine, text, foregroundColor, backgroundColor);
        }

        public static void Write(
            string text = "",
            ConsoleColor? foregroundColor = null,
            ConsoleColor? backgroundColor = null)
        {
            WriteToConsoleWith(Console.Write, text, foregroundColor, backgroundColor);
        }

        private static void WriteToConsoleWith(
            Action<string> consoleAction,
            string text,
            ConsoleColor? foregroundColor,
            ConsoleColor? backgroundColor)
        {
            var originalFgColor = Console.ForegroundColor;
            var originalBgColor = Console.BackgroundColor;

            Console.ForegroundColor = foregroundColor ?? originalFgColor;
            Console.BackgroundColor = backgroundColor ?? originalBgColor;

            consoleAction(text);

            Console.ForegroundColor = originalFgColor;
            Console.BackgroundColor = originalBgColor;
        }
    }
}