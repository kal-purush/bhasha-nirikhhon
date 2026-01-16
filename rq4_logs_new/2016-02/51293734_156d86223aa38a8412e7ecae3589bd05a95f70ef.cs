using System;

namespace SayAndAsk
{
    class Program
    {
        static void Main(string[] args)
        {
            // string Answer;
            Say("Hello");
            // Answer = Ask("Are you haveing a nice day?");
            Repeat("Are you haveing a nice day?");
            

            Console.Read();
        }

        static void Repeat(string question)
        {
            Say(Ask(question));
        }

        static string Ask(string question)
        {
            Say(question);
            return Console.ReadLine();
        }

        static void Say(string message)
        {
            Console.WriteLine(message);
        }
    }
}