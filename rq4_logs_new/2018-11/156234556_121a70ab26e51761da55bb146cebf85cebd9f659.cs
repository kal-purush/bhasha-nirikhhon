using System;
using System.Collections.Generic;
using UtilityLibraries;

namespace GB1Lesson6
{
    internal partial class Program
    {

        private enum MenuKey
        {
            Task1 = 1,
            Task2 = 2,
            Exit = 8,
        }

        private enum TaskReturnCode
        {
            Exit,
            Continue
        }

        private static IReadOnlyDictionary<uint, string> MenuTextByMenuKey = new Dictionary<uint, string> {
            { (byte)MenuKey.Task1, "Run Task1 solution"},
            { (byte)MenuKey.Task2, "Run Task2 solution"},
            { (byte)MenuKey.Exit, "exit program"},
        };

        private static IReadOnlyDictionary<uint, Func<TaskReturnCode>> MenuActionByMenuKey = new Dictionary<uint, Func<TaskReturnCode>> {
            { (byte)MenuKey.Task1, () => ExecuteTask1Solution()},
            { (byte)MenuKey.Task2, () => ExecuteTask2Solution()},
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
                uint UserChoice = ConsoleHelper.PrintMenuAndReadChoice(MenuTextByMenuKey);
                if (MenuActionByMenuKey.ContainsKey(UserChoice))
                {
                    CurrentTaskReturnCode = MenuActionByMenuKey[UserChoice]();
                }
                else
                {
                    ConsoleHelper.PrintAndPause("Invalid choise");
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