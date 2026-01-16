/*
    Team Treehouse
    Current Version - 1.0
    Date - 12/12/18
    Author - Clayton Lewis

    Version 1.0 - 12/12/18
        - Testing and setting up VSCode
*/

using System;

namespace Treehouse.FitnessFrog
{
    class Console_Project
    {
        static void Main()
        {
            double runningTotal = 0;
            
            while (true) {
                // Prompt user for minutes exercised 
                Console.Write("Enter how many minutes you exercised or type \"quit\" to exit: ");            
                string entry = Console.ReadLine();

                if (entry.ToLower() == "quit") {
                    break;
                }
                
                try {
                    // Add minutes exercised to total
                    double minutes = double.Parse(entry);

                    if (minutes <= 0) {
                        Console.WriteLine(minutes + " is not an acceptable value.");
                        continue;
                    }
                    else if (minutes <= 10) {
                        Console.WriteLine("Better than nothing, am I right?");
                    }
                    else if(minutes <= 30) {
                        Console.WriteLine("Way to go hot stuff!");
                    }
                    else if(minutes <= 60) {
                        Console.WriteLine("You must be a ninja warrior in training!");
                    }
                    else {        
                        Console.WriteLine("Okay, now you're just showing off!");
                    }

                    runningTotal = runningTotal + minutes;

                    // Display total minutes exercised to the screen 
                    Console.WriteLine("You've entered " + runningTotal + " minutes.");

                    // Repeat until user quits
                }
                catch (FormatException) {
                    Console.WriteLine("That is not valid input");
                }
            }

            Console.WriteLine("Goodbye");
        }
    }
}