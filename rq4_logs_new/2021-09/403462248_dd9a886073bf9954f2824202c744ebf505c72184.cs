using System;

namespace SelectionStatementExercise
{
    class Program
    { }
        namespace SelectionStatementsExercise
        {
        class Program
        {
            static void Main(string[] args)
            {
                var favNumber = 25;

                Console.WriteLine("Try to guess my favorite number");
                var userInput = int.Parse(Console.ReadLine());

                if (userInput < 25)
                {
                    Console.WriteLine("Too low");
                }

                else if (userInput > 25)
                {
                    Console.WriteLine("Too high");
                }

                else if (userInput == 25)
                {

                    Console.WriteLine("You are correct!");
                }
                else
                {
                    Console.WriteLine("Nevermind");
                }

                Console.WriteLine("What is your favorite subject?");

                var subject = Console.ReadLine();

                switch (subject)
                {
                    case "Chemistry":
                        Console.WriteLine("Interesting, Chemistry is hard!");
                        break;
                    case "English":
                        Console.WriteLine("English is pretty boring and I don't like writing essays");
                        break;
                    case "Biology":
                        Console.WriteLine("Biology is awesome, I love learning about the science of life");
                        break;
                    case "Art":
                        Console.WriteLine("I don't like drawing");
                        break;
                    case "Social Studies":
                        Console.WriteLine("Social Studies is also pretty boring");
                        break;
                    default:
                        Console.WriteLine("That subject doesn't interest me");
                        break;






                }







            }


        }




    }



}
