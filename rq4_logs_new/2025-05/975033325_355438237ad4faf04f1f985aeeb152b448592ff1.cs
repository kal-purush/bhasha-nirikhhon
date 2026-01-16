namespace Practice2.Task22
{
    internal class Program
    {
        static bool StopGame;
        static int userImput;


        static void gameStart()
        {
            
        }
        static void Main(string[] args)
        {
            Console.WriteLine("Игра 'Угадай число'");
            Console.WriteLine();
            Console.WriteLine("Для выхода из игры нажмите клавишу F");
            do
            {
                
                Random random = new Random();
                int rand = random.Next(1, 100);
                Console.WriteLine("Введите число от 1 до 100 и нажмите Enter: ");

                int userCount = 0;
                do
                {
                    
                    userImput = Convert.ToInt32(Console.ReadLine());
                    if (rand == userImput)
                    {
                        Console.WriteLine("Поздравляем, вы угадали число");
                        break;
                    }
                    else if (rand > userImput)
                    {
                        Console.WriteLine("К сожалению вы ошиблись, загаданное число больше");
                    }
                    else
                    {
                        Console.WriteLine("К сожалению вы ошиблись, загаданное число меньше");
                    }

                    userCount++;
                    int numberOfAttempts = 10 - userCount;
                    Console.WriteLine($"У вас осталось {numberOfAttempts} попыток");


                    if (userCount == 10)
                    {
                        Console.WriteLine("Ваши попытки закончены, вы проиграли");
                        break;
                    }
                    if (Console.ReadKey(true).Key == ConsoleKey.F)
                    {
                        break;
                    }
                } while (rand != userImput);
                Console.WriteLine("Хотите сыграть снова?(Да/Нет)");
                string game = (string)Console.ReadLine();
                StopGame = (game == "Да" || game == "да") ? true : false;
            } while (StopGame == true);
            
            
        }
    }
}