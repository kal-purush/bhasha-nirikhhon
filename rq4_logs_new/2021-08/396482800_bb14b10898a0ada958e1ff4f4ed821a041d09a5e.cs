using System;

namespace HomeworkMargaret.General
{
    class Homework1_Variables
    {
        public static void Start()
        {
            Console.WriteLine("Start");
            Console.WriteLine("Hello, Gotham Fan! How can I call you?");
            string callMe = Console.ReadLine();
            Console.WriteLine();

            Console.WriteLine("Great " + callMe + "! Tell me, who is your fave character in Gotham?");
            string faveCharacter = Console.ReadLine();
            Console.WriteLine();

            Console.WriteLine("Wow! Why do you like this one?");
            string features = Console.ReadLine();
            Console.WriteLine();

            Console.WriteLine("Ok. And what about your fave season?");
            string faveSeason = Console.ReadLine();
            Console.WriteLine();

            Console.WriteLine("Why do you like this " + faveSeason + " so much?");
            string featSeason = Console.ReadLine();
            Console.WriteLine();

            Console.WriteLine("And how many times have you watched Gotham?");
            string times = Console.ReadLine();
            Console.WriteLine();

            Console.WriteLine("Only " + times + "! You must watch it again to be a real fan!");
            Console.WriteLine();

            Console.WriteLine("So, and what about DC films? Which one is you favorite?");
            string faveFilm = Console.ReadLine();
            Console.WriteLine();

            Console.WriteLine("Great choice! I love " + faveFilm + " too!");
            Console.WriteLine("Well, " + callMe + " thanks for chatting with me");
        }
    }
}