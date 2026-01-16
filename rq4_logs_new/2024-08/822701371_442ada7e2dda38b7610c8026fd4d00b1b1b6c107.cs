namespace Practice7_ThreadsConsole
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            ThreadController threadController = new ThreadController();
            while (true)
            {
                int numberOfThreads = int.Parse(Console.ReadLine());
                if (numberOfThreads > 0)
                {
                    for (int i = 0; i < numberOfThreads; i++)
                    {
                        threadController.CreateThread($"Thread {i + 1}");
                    }
                }
                else
                {
                    Console.WriteLine("Введите корректное число.");
                }
            }
        }
    }
}