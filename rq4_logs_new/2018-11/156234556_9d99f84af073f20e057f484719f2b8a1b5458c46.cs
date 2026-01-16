using System;
using System.Collections.Generic;
using System.Linq;
using UtilityLibraries;

namespace GB1Lesson5
{
    internal partial class Program
    {

        private enum MenuKey
        {
            Task3 = 1,
            Task4 = 2,
            Exit = 8,
        }

        private enum TaskReturnCode
        {
            Exit,
            Continue
        }

        private static IReadOnlyDictionary<uint, string> MenuTextByMenuKey = new Dictionary<uint, string> {
            { (byte)MenuKey.Task3, "Run Task3 solution"},
            { (byte)MenuKey.Task4, "Run Task4 solution"},
            { (byte)MenuKey.Exit, "exit program"},
        };

        private static IReadOnlyDictionary<uint, Func<TaskReturnCode>> MenuActionByMenuKey = new Dictionary<uint, Func<TaskReturnCode>> {
            { (byte)MenuKey.Task3, () => ExecuteTask3Solution()},
            { (byte)MenuKey.Task4, () => ExecuteTask4Solution()},
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