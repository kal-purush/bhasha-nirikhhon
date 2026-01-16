using System;

namespace DEVTask8
{
    /// <summary>
    /// Class-command for getting humans' average age in Storage
    /// </summary>
    class AverageAge : ICommand
    {
        private Storage storage;

        public AverageAge(Storage paramStorage)
        {
            storage = paramStorage;
        }

        public void Execute()
        {
            int averageAge = storage.AverageAge();
            Console.WriteLine($"Average age : {averageAge}");
        }
    }
}