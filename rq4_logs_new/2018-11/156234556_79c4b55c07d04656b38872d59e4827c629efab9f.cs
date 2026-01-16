using System;
using System.Collections.Generic;

namespace GB1Lesson2
{
    internal partial class Program
    {
        private enum MenuKey
        {
            Task1 = 1,
            Task2 = 2,
            Task3 = 3,
            Task4 = 4,
            Task5 = 5,
            Task6 = 6,
            Task7 = 7,
            Exit = 8,
        }

        private enum TaskReturnCode
        {
            Exit,
            Continue
        }

        private static IReadOnlyDictionary<uint, string> MenuTextByMenuKey = new Dictionary<uint, string> {
            { (byte)MenuKey.Task1, "execute Task1"},
            { (byte)MenuKey.Task2, "execute Task2"},
            { (byte)MenuKey.Task3, "execute Task3"},
            { (byte)MenuKey.Task4, "execute Task4"},
            { (byte)MenuKey.Task5, "execute Task5"},
            { (byte)MenuKey.Task6, "execute Task6"},
            { (byte)MenuKey.Task7, "execute Task7"},
            { (byte)MenuKey.Exit, "exit program"},
        };

        private static IReadOnlyDictionary<uint, Func<TaskReturnCode>> MenuActionByMenuKey = new Dictionary<uint, Func<TaskReturnCode>> {
            { (byte)MenuKey.Task1, () => ExecuteTask1Solution()},
            { (byte)MenuKey.Task2, () => ExecuteTask2Solution()},
            { (byte)MenuKey.Task3, () => ExecuteTask3Solution()},
            { (byte)MenuKey.Task4, () => ExecuteTask4Solution()},
            { (byte)MenuKey.Task5, () => ExecuteTask5Solution()},
            { (byte)MenuKey.Task6, () => ExecuteTask6Solution()},
            { (byte)MenuKey.Task7, () => ExecuteTask7Solution()},
            { (byte)MenuKey.Exit, () => ExitProgram()},
        };

        private static void Main(string[] args)
        {
            PrintMenuUntilExitKeyNotPressed();
        }

        private static void PrintMenuUntilExitKeyNotPressed()
        {
            TaskReturnCode CurrentTaskReturnCode = TaskReturnCode.Continue;
            do
            {
                uint UserChoice = UtilityLibraries.ConsoleHelper.PrintMenuAndReadChoice(MenuTextByMenuKey);
                if (MenuActionByMenuKey.ContainsKey(UserChoice))
                {
                    CurrentTaskReturnCode = MenuActionByMenuKey[UserChoice]();
                } else
                {
                    Console.WriteLine("Invalid choise");
                    Console.ReadKey();
                }
            } while (CurrentTaskReturnCode != TaskReturnCode.Exit);
        }

        private static TaskReturnCode ExitProgram()
        {
            Console.WriteLine("Bye! See you tomorrow!");

            return TaskReturnCode.Exit;
        }
    }
}