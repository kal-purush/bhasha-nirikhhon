using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlindDate
{
    class blindDate
    {
        static void Main(string[] args)
        {
            gameTitle();

        }

        public static void gameTitle()
        {
            Console.ForegroundColor = ConsoleColor.DarkRed;
            Console.WriteLine("Welcome to your blind date!");
            Console.WriteLine("Press Enter to begin.");
            Console.ReadLine();
            Console.Clear();
            first();
        }



        public static void first()
        {
            string choice;
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(" Your date gets up and you assume they have gone to the bathroom. 20 minutes later, they are still not back....");
            Console.WriteLine(" What do you do? ");
            Console.WriteLine("1. Go looking for them ");
            Console.WriteLine("2. Leave, what a waste of time! ");
            Console.WriteLine("3. Cry. ");
            Console.WriteLine("4. Ask them what the heck they are doing. ");
            Console.Write("Choice: 1 - 4 ");
            //ToLower () makes user input into lowercase
            choice = Console.ReadLine().ToLower();
            Console.Clear();

            switch (choice)
            {
                //you can call multiple case scenarios for the same block of code
                case "1":
                case "go looking for them":
                case "go look for them":
                    {
                        Console.ForegroundColor = ConsoleColor.DarkYellow;
                        Console.WriteLine(" You start looking and find your date. There they are and it appears they are serving tables....they are working a shift during your date. ");
                        Console.WriteLine(" Why.... ");
                        Console.WriteLine(" You must be making a face because your date starts to approach you... ");
                        Console.WriteLine(" You quickly leave the restuarant ");
                        Console.WriteLine(" Press 'ENTER' to continue. ");
                        Console.ReadLine();
                        gameOver();
                        break;
                    }


                case "2":
                case "leave":
                    {
                        Console.ForegroundColor = ConsoleColor.Magenta;
                        Console.WriteLine(" You gather your things and are heading out the door...");
                        Console.WriteLine(" You've shrugged off what you believe was a bad date. You walk onward with your night. ");
                        Console.WriteLine(" Press Enter to continue....");
                        Console.ReadLine();
                        second();
                        break;
                    }


                case "3":
                case "cry":
                    {
                        Console.ForegroundColor = ConsoleColor.DarkBlue;
                        Console.WriteLine(" You start to tear and cry a little...");
                        Console.WriteLine(" Through your tears, you see the EXIT sign. " +
                            "\nIt's at this time you decide to leave. " +
                            "\nYou start walking outside along the sidewalk. ");
                        Console.WriteLine(" Press Enter to continue....");
                        Console.ReadLine();
                        second();
                        break;
                    }
                case "4":
                case "ask":
                    {
                        Console.ForegroundColor = ConsoleColor.DarkBlue;
                        Console.WriteLine("You alarm your date when you approach them");
                        Console.WriteLine("It feels as if they have completely forgotten about your date. ");
                        Console.WriteLine(" Press Enter to continue....");
                        Console.ReadLine();
                        fourth();
                        break;
                    }
                default:
                    {

                        {
                            Console.ForegroundColor = ConsoleColor.Cyan;
                            Console.WriteLine(" I don't understand that command....");
                            Console.WriteLine(" Press 'Enter' to try again");
                            Console.ReadLine();
                            //call back to the first() to repeat question
                            first();
                            break;
                        }
                    }

            }


        }




        public static void second()
        {
            Console.ForegroundColor = ConsoleColor.DarkBlue;
            Random rnd = new Random();
            //string together random scenarios in an array to initiate a randomizer
            //this will pull a random index out of the array to get a random second statement 
            string[] secOptions = { " You begin walking down the street, you see glowing yellow eyes. ", " You begin walking down the street, you hear loud vibey music. ", " You begin walking down the street, you see beautiful lights in the distance. " };
            int randomNumber = rnd.Next(0, 3);
            string secText = secOptions[randomNumber];
            //this will pick a random index out of that array COOOL

            string secChoice;
            //outputting the secText variable 
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine(secText);
            Console.WriteLine(" Do you continue forward? ");
            Console.Write("Choice: yes or no ");
            //put user's choice into the secChoice variable
            secChoice = Console.ReadLine().ToLower();

            if (secChoice == "yes" || secChoice == "y")
            {
                third();
            }
            else if (secChoice == "no" || secChoice == "n")
            {
                Console.ForegroundColor = ConsoleColor.DarkRed;
                Console.WriteLine(" A meteor slams straight into the road at that exact moment, killing you instantly. ");
                Console.WriteLine(" Press 'Enter' to continue.");
                Console.ReadLine();
                gameOver();
            }
            //call another else, it's not 'N', it's not 'no', it's something unrecognized
            else
            {
                Console.ForegroundColor = ConsoleColor.DarkGreen;
                Console.WriteLine(" I don't understand that command....");
                Console.WriteLine(" Press 'Enter' to try again");
                Console.ReadLine();
                //call back to the second() to repeat question
                second();
            }

        }
        public static void third()
        {
            int age;
            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Console.WriteLine(" You start running towards what you see and you run right into a RAVE! ");
            Console.WriteLine(" SUUURRRPPPRRRIIISSSEEE RAAAAAAVVVVVVVEEEE! ");
            Console.WriteLine(" You can't get in unless you're the correct age. ");
            Console.WriteLine(" Age: ");
            //asking console to parse this string into a number
            int.TryParse(Console.ReadLine(), out age);

            //this while loop will continue to loop until user guesses 42
            while (age < 42)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine(" Seriously? I am not an idiot, to get in I need your correct age. \nIt's always the answer. ");
                Console.WriteLine(" Come on, how old are you really? ");
                Console.Write("Age: ");
                int.TryParse(Console.ReadLine(), out age);
            }



            Console.WriteLine(" Wow, that wasn't so hard. You ready to dance? ");
            youWin();
        }

        public static void fourth()
        {
            Console.ForegroundColor = ConsoleColor.DarkBlue;
            Random rnd = new Random();
            //string together random scenarios in an array to initiate a randomizer
            //this will pull a random index out of the array to get a random second statement 
            string[] fourOptions = { "Your date explains to make ends meet at their self owned shampoo company, they must work tonight and did in fact forget about you.", "Your date explains they are great at multi-tasking an do not see the concern for working during your date.", "Your date begins to explain themselves but you've decided you've wasted enough time." };
            int randomNumber = rnd.Next(0, 3);
            string fourText = fourOptions[randomNumber];
            //this will pick a random index out of that array COOOL

            string fourthChoice;
            //outputting the secText variable 
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine(fourText);
            Console.WriteLine(" Do you continue forward? ");
            Console.Write("Choice: yes or no ");
            //put user's choice into the secChoice variable
            fourthChoice = Console.ReadLine().ToLower();

            if (fourthChoice == "yes" || fourthChoice == "y")
            {
                third();
            }
            else if (fourthChoice == "no" || fourthChoice == "n")
            {
                Console.ForegroundColor = ConsoleColor.DarkRed;
                Console.WriteLine(" A meteor slams straight into the road at that exact moment, killing you instantly. ");
                Console.WriteLine(" Press 'Enter' to continue.");
                Console.ReadLine();
                gameOver();
            }
            //call another else, it's not 'N', it's not 'no', it's something unrecognized
            else
            {
                Console.ForegroundColor = ConsoleColor.DarkGreen;
                Console.WriteLine(" I don't understand that command....");
                Console.WriteLine(" Press 'Enter' to try again");
                Console.ReadLine();
                //call back to the second() to repeat question
                second();
            }
        }


        public static void gameOver()
        {
            Console.Clear();
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine("This date did not go as expected...." +
                              "\nBetter luck next time. ");
            Console.WriteLine("Press 'Enter' to try again");
            Console.ReadLine();
            Console.Clear();
            gameTitle();
        }

        public static void youWin()
        {
            Console.Clear();
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine("You danced the night away and decide blind dates are overrated." +
                              "\nMaybe you'll get a cat or two.");
            Console.WriteLine("\nPress 'Enter' to try again");
            Console.ReadLine();
            Console.Clear();
            gameTitle();

        }





    }
}