using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace chapter_08.controller
{
    class MainController
    {
        private const int EXIT_PROCESS = 999;

        public static void Run()
        {
            try
            {
                while (true)
                {
                    int employeeId = getEmployeeId();
                    if (employeeId == EXIT_PROCESS)
                    {
                        break;
                    }
                    else if (!IsValidEmployeeId(employeeId))
                    {
                        Console.WriteLine($"社員番号 {employeeId} さんは課題実施対象者ではありません。");
                    }
                    else
                    {
                        Console.WriteLine($"{employeeId} さんの課題を確認します。");
                        MakeCharacter(employeeId);
                        break;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                System.Environment.ExitCode = 1;
            }
            finally
            {
                Console.WriteLine("プログラムを終了します。");
            }
        }

        private static int getEmployeeId()
        {
            Console.WriteLine("誰の課題を確認しますか？");
            Console.Write("社員番号(999で終了): ");
            var input = Console.ReadLine();
            if (int.TryParse(input, out int employeeId))
            {
                return employeeId;
            }
            else
            {
                throw new FormatException("入力文字列が正しい形式ではありませんでした。");
            }
        }

        private static bool IsValidEmployeeId(int employeeId)
        {
            List<int> employeesList = new List<int> { 92, 667 };
            return employeesList.Any(x => x.Equals(employeeId));
        }

        private static void MakeCharacter(int employeeId)
        {
            Console.WriteLine($"{employeeId} さんの課題");
            Console.WriteLine("-------------------");

            switch (employeeId)
            {
                case 92:
                    break;
                case 667:
                    break;
                default:
                    throw new ArgumentNullException($"{employeeId}さんの課題は存在しません。");
            }
        }
    }
}