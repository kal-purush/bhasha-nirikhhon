using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Week3CodingChallenge.InputManagement;

namespace Week3CodingChallenge
{
    public static class EvenCheck
    {
        public static void CheckEven(IUserInput input)
        {
            //variable to hold the user's input prior to validation
            String userInput = "";
            int parsedUserInput = 0;

            //Activity explanation
            Console.Clear();
            Console.WriteLine("This activity will take an integer from you, then tell you if it is even or not!");

            //This loop contains the user in the method, ensuring they submit valid inputs
            do
            {
                Console.Write("Please enter an integer to be checked: ");

                //set userInput to the user's input
                userInput = input.GetInput();

                //Check if string is empty or a bunch of white space
                if (!String.IsNullOrWhiteSpace(userInput))
                {
                    try
                    {
                        //parse user input to int
                        parsedUserInput = int.Parse(userInput);

                        if (parsedUserInput % 2 == 0) //Input is even (divisible by 2 with no remainder)
                        {
                            Console.WriteLine($"{parsedUserInput} is an even number.");
                        }
                        else //input is odd (divisible by two with some remainder)
                        {
                            Console.WriteLine($"{parsedUserInput} is not an even number.");
                        }

                        //This code block returns out of the method on successful completion.
                        Console.WriteLine("\nPress enter when you are done to return to the main menu.");
                        input.GetInput();
                        return;
                    }
                    catch (FormatException) //Input could not be parsed to int due to format
                    {
                        try
                        {
                            double.Parse(userInput); //Check if user input is a double
                            Console.WriteLine($"{userInput} is a double, not an integer.");
                        }
                        catch (FormatException)
                        {
                            //User input could not be translated to a number
                            Console.WriteLine($"{userInput} is a string, not an integer.");
                        }
                    }
                    catch (Exception) //Catch any other exceptions that may spring up
                    {
                        Console.WriteLine($"An unhandled expression occurred from user input: {userInput}");
                    }
                }
                else //user input is empty or white space
                {
                    Console.WriteLine("Data input must contain characters. Please check your input and try again.");
                }

            } while (true);
        }
    }
}