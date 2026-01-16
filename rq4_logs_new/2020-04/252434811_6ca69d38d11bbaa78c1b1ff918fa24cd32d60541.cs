using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HomeWork
{
    public static class Work6
    {
        public static void Start()
        {
            Stack<string> stack = new Stack<string>();

            

            int numb;

            do
            {
                Console.WriteLine("Выберите метод работы со стеком\n" +
                "0 - выход\n" +
                "1 - добавление элемента\n" +
                "2 - изъятие элемента");

                int.TryParse(Console.ReadLine(), out numb);
                switch (numb)
                {
                    case 1:
                        {
                            Console.WriteLine("Введите что хотите внести в стек");
                            string str = Console.ReadLine();
                            if (!string.IsNullOrEmpty(str))
                            {
                                stack.Push(str);
                            }    
                        }
                        continue;
                    case 2:
                        if (stack.Count > 0)
                        {
                            Console.WriteLine("Верхний элемент: " + stack.Pop());
                        }
                        else Console.WriteLine("Стек пустой");
                        
                        continue;
                    default:
                        break;
                }
            } while (numb != 0);

            
            //var nextInStack = stack.Pop();
            //var peekResult = stack.Peek();

            //Console.WriteLine(nextInStack);

            //Queue<string> queue = new Queue<string>(new[] { "1", "2" });
            //var nextInQueue = queue.Dequeue();



            //Console.WriteLine(nextInQueue);
        }
    }
}