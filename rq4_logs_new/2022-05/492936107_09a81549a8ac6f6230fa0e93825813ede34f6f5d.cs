using System;

namespace Homework3
{
    class MainClass
    {
        public static void Main()

        {
            int numTask;

            while (true)
            {
                Console.WriteLine("===============================");
                Console.WriteLine("1 - Задача 1");
                Console.WriteLine("2 - Задача 2");
                Console.WriteLine("3 - Задача 3");
                Console.WriteLine("4 - Задача 4\n");
                Console.WriteLine("0 - Завершить работу приложения");
                Console.WriteLine("===============================\n");
                Console.Write("Укажите номер задачи: ");
                numTask = int.Parse(Console.ReadLine());
                Console.WriteLine();

                switch (numTask)
                {
                    case 0:
                        return;

                    case 1:
                        int[,] array = new int[3, 3]
                        {
                            {1,2,3 },
                            {4,5,6 },
                            {7,8,9 }
                        };
                        Task1(array);
                        break;

                    case 2:
                        Task2();
                        break;

                    case 3:
                        Task3();
                        break;

                    case 4:
                        Task4();
                        break;

                    default:
                        Console.WriteLine("Вы ввели неккореткный номер задачи");
                        Console.WriteLine("Повторите попытку ввода!");
                        break;

                }

            }




        }
        /// <summary>
        /// Задача №1 Написать программу, выводящую элементы двухмерного
        /// массива по диагонали.
        /// </summary>
        public static void Task1(int[,] array)
        {

            int next_string = 0;

            for (int i = 0; i < array.GetLength(0); i++)
            {
                for (int j = 0; j < array.GetLength(1); j++)
                {
                    array[i, j] = next_string + 1;
                    //Console.WriteLine($"{array[i, j]}"); 
                    Console.WriteLine($"{new string(' ', next_string)}{array[i, j]}");
                    next_string++;



                }
            }
        }



        /// <summary>
        /// Задача №2 создать двумерный массив 5*2, хранящий список
        /// телефонных контактов: первый элемент хранит имя контакта,
        /// второй — номер телефона/e-mail.
        /// </summary>
        public static void Task2()
        {

            string[,] test = new string[5, 2]
            {
                {"Tony","+7(916)837-54-44"},
                {"Josh","+7(901)504-32-11"},
                {"Mike","+7(906)123-67-89"},
                {"Boris","+7(910)435-90-12"},
                {"Bruce","+7(915)674-13-69"},

            };

            while (true)
            {
                Console.WriteLine("=================================");
                Console.WriteLine("Выбирите номер контакта от 1 до 5");
                Console.WriteLine("Для выхода в основное меню нажмите 0");
                Console.WriteLine("====================================");
                Console.Write("Введите номер контакта: ");
                int numCon = int.Parse(Console.ReadLine());
                --numCon;
                


                if (numCon == -1)
                {
                    Console.WriteLine("Выход в основное меню . . .");
                    break;
                }
                else if(numCon < -1 || numCon >= test.GetLength(0))
                {
                    Console.WriteLine("Введены неверные данные!");
                    Console.WriteLine("Попробуйте еще раз.");
                    Console.WriteLine();
                    
                }
                else
                {

                    for (int i = 0; i < test.GetLength(0); i++)
                    {
                        if (i == numCon)
                        {
                            for (int j = 0; j < test.GetLength(1); j++)
                            {
                                Console.Write($"{test[i, j]} ");
                            }
                            Console.WriteLine();
                        }
                       
                    }
                    Console.WriteLine();

                }
                
            }
        }

      
        


        /// <summary>
        /// Задача №3 Написать программу, выводящую введенную пользователем строку в обратном порядке
        /// </summary>
        public static void Task3()
        {
            Console.Write("input words: ");
            string words = Console.ReadLine();
            for (int i = words.Length - 1; i >= 0; i--)
            {
                Console.Write(words[i]);
            }
            Console.WriteLine();


        }



        /// <summary>
        /// вывести на экран массив 10х10, состоящий из символов X и O, где Х элементы кораблей
        /// , а О — свободные клетки.
        /// </summary>
        public static void Task4()
        {

            char[,] seaBattle = new char[10, 10]
            {
                {'O','X','O','O','O','O','O','X','O','O', },
                {'O','X','O','O','O','O','O','X','O','O', },
                {'O','X','O','O','O','O','O','O','O','X', },
                {'O','X','O','O','O','O','O','O','O','X', },
                {'O','O','O','O','O','O','O','O','O','X', },
                {'O','O','X','X','X','O','O','O','O','O', },
                {'O','O','O','O','O','O','O','X','O','O', },
                {'O','O','O','O','O','O','O','O','O','O', },
                {'O','O','O','O','O','O','O','O','O','O', },
                {'O','O','O','O','O','X','X','X','O','O', }

            };
            for (int i = 0; i < seaBattle.GetLength(0); i++)
            {
                for (int j = 0; j < seaBattle.GetLength(1); j++)
                {
                    Console.Write($"{seaBattle[i, j]} ");
                }
                Console.WriteLine();

            }
        }
    }
}