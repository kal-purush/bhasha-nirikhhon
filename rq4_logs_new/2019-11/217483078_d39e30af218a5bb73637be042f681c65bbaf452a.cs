using System;

namespace Module_3_Task_1
{
    class Program
    {
        static int ReadWithCheckInt()
        {
            int num = 0;
            bool check = int.TryParse(Console.ReadLine(), out num);
            while (!check)
            {
                Console.WriteLine("Некорректно, еще раз");
                check = int.TryParse(Console.ReadLine(), out num);
            }
            return num;
        }
        static void Main(string[] args)
        {
            
            Console.WriteLine("Введите первое число:");
            int firstNum=ReadWithCheckInt();
            
            Console.WriteLine("Введите второе число:");
            int secondNum=ReadWithCheckInt();

            int result = 0;
            if (firstNum != 0 && secondNum != 0)
            {

                int secondAbs = Math.Abs(secondNum);
                

                for (int i = 0; i < secondAbs; i++)
                {
                    result += firstNum;
                }

                if((result<0 && secondNum<0)||(result>0 && secondNum<0))
                {
                    result = -result;
                }
            }
            else
            {
                result = 0;
            }
            Console.WriteLine($"result = {result}");
            Console.ReadKey();
        }
    }
}