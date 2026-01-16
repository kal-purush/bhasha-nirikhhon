using System;

namespace Excercise9
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Write("正の整数を入力してください：");
            var inputText = Console.ReadLine();
            Console.WriteLine("入力値：" + inputText);

            var inputBytes = System.Text.Encoding.UTF8.GetBytes(inputText);
            var inputNumber = 0.0;
            for (var i = 0; i <= inputBytes.Length - 1; i++)
            {
                if (inputBytes[i] <= 47 || inputBytes[i] > 57)
                {
                    Console.WriteLine("変換できません！！！");
                    return;
                }
                inputNumber += (inputBytes[i] - 48) * Math.Pow(10, inputBytes.Length - 1 - i);
            }

            Console.WriteLine("変換値：" + inputNumber);
        }
    }
}