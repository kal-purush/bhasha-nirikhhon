using System;

namespace Homework_2
{
    class MainClass
    {
  


        public static void Main(string[] args)
        {   //Задача №1
            Console.Write("Введите миниальную температуру: ");
            int a = int.Parse(Console.ReadLine());
            Console.Write("Введите максимальную температуру: ");
            int b = int.Parse(Console.ReadLine());
            int temp = TempAVG(a,b);



            //Задача №2
           
            int month = nameMonth();



            //Задача №3
            //Определить, является ли введённое пользователем число чётным.
            if (month % 2 == 0)
            {
                Console.WriteLine("Вы ввели четное число");
            }
            else
            {
                Console.WriteLine("Вы ввели нечетное число");
            }


            //Задача №5
            if (month == 12 || month == 1 || month == 2 && temp > 0)
                Console.WriteLine("Дождливая зима");



            //Задача №4
            Check();

           




        }

        /// <summary>
        /// Задача №1
        /// Вычисление средней температуры
        /// </summary>

        public static int TempAVG(int a, int b)
        {
            int avg = (a + b) / 2;
            Console.WriteLine("Средня температура равна: " + avg);
            return avg;
        }



        /// <summary>
        /// Задача №2
        /// Вычесление названия месаца по введенному значению
        /// </summary>
         public static int nameMonth()
        {

            Console.Write("Укажите порядковый номер месяца: ");
            int monthNo = int.Parse(Console.ReadLine());
            if (monthNo < 1 || monthNo > 12)
            {
                Console.WriteLine("Вы указали некорректный номер месяца.");
            }
            Console.WriteLine($"Вы указали месяц: {new DateTime(1, monthNo, 1).ToString("MMMM")}");
           

            return monthNo;

        }

        /// <summary>
        /// Задача № 4
        /// </summary>
        public static void Check()
        {

            int num_chek = 123098;
            int inn = 503235678;
            string name = "ООО Бакинский дворик";
            string address = "ул. Добрынинская, д 35";
            string milk = "Молоко";
            int price_milk = 120;
            string flakes = "Хлопья";
            int price_flakes = 346;
            float summ = 500.00f;
            float change = 34.00f;
            DateTime date = DateTime.Now;


            Console.WriteLine($"        Кассовый чек {num_chek}     \n" +
                              $"        {name}                      \n" +
                              $"        ИНН {inn}                   \n" +
                              $"        Адрес {address}             \n" +
                              $"        Дата: {date}                \n" +
                              $"------------------------------------\n" +
                              $"Приход                              \n" +
                              $"Продажа                             \n" +
                              $"------------------------------------\n" +
                              $"Товар   Цена    Количество          \n" +
                              $"{milk}  {price_milk}     1          \n" +
                              $"{flakes}  {price_flakes}     1      \n" +
                              $"------------------------------------\n" +
                              $"Наличными   {summ}                  \n" +
                              $"Сдача       {change}                \n" +
                              $"------------------------------------\n");


        }
    }
}