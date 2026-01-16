namespace exrcise7
{
    using System;
    using System.Collections.Generic;
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Valitse numero: Kivi = 1; Sukset = 2; Paperi = 3;");
            int userAnswer = Int32.Parse(Console.ReadLine());

            List<int> list = new List<int>{1,2,3};
            Random random = new Random();
            int index = random.Next(list.Count);
            Console.WriteLine(list[index]);

            if(userAnswer == 1 && list[index] == 2)
            {
                Console.WriteLine("User won!");
            }
            else if(userAnswer == 2 && list[index] == 3)
            {
                Console.WriteLine("User won!");
            }
            else if(userAnswer == 3 && list[index] == 2)
            {
                Console.WriteLine("User won!");
            }
            else if(userAnswer == 2 && list[index] == 1) 
            {
                Console.WriteLine("Computer won!");
            }
            else if(userAnswer == 3 && list[index] == 2)
            {
                Console.WriteLine("Computer won!");
            }
            else if(userAnswer == 1 && list[index] == 3)
            {
                Console.WriteLine("Computer won!");
            }
        }
    }
}