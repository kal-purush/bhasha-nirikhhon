namespace MagicVilla.Logging
{
    public class LoggingV2 : ILogging
    {
        public void Log(string message, string type)
        {
            if (type == "error")
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"ERROR - {message}");
                Console.ForegroundColor = ConsoleColor.Black;
            }
            else
            {
                if (type == "warning")
                {
                    Console.ForegroundColor = ConsoleColor.DarkYellow;
                    Console.WriteLine($"WARNING - {message}");
                    Console.ForegroundColor = ConsoleColor.Black;
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine($"INFORMATION - {message}");
                    Console.ForegroundColor = ConsoleColor.Black;
                }
            }

        }
    }
}