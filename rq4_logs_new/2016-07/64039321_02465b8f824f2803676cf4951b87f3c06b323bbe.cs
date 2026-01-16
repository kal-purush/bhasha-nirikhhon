using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GuessingGame
{

    class GuessGame
    {
        private int randomVar;
        private Dictionary<String, String> GGString = new Dictionary<string, string>();
        private Dictionary<String, int> GGInt = new Dictionary<string, int>();
        private Random random = new Random();
        int pickNumber;


        public void MessageAssignment()
        {

            this.GGString.Add("startGame", "start the game");
            this.GGString.Add("failMessage", "sorry game over");
            this.GGString.Add("winMessage", "CONGRATULATIONS YOU WON THE GAME");
            this.GGString.Add("infMessage", "Number provided is less than the number picked by the program,... Please try again!.");
            this.GGString.Add("supMessage", "Number provided is greater than the number picked by the program,... Please try again!.");
            this.GGString.Add("PlayAgainMessage", "Would you like to play again? Y/N.");

            this.GGInt.Add("score", 100);
            this.GGInt.Add("lives", 10);

        }

        public int startGame()
        {


            int marker = 0;
            int score;
            int lives = 10;
            String playAgain = "NULL";
            String printValue0;
            String printValue1;
            String printValue2;
            String printValue3;


            Console.WriteLine("#######################################################");

            GGString.TryGetValue("startGame", out printValue0);
            Console.WriteLine("\n" + printValue0);

            //System Number picking
            this.randomVar = this.random.Next(1, 100);
            Console.WriteLine("random number is {0}", randomVar);





            while (lives > 0)
            {

                //Human Number picking
                Console.Write("Enter your number: ");
                this.pickNumber = Convert.ToInt32(Console.ReadLine());

                if (pickNumber < randomVar)
                {

                    GGString.TryGetValue("infMessage", out printValue1);
                    Console.WriteLine("\n" + printValue1);
                    GGInt["score"] -= 10;
                    GGInt["lives"] -= 1;

                    GGInt.TryGetValue("score", out score);
                    GGInt.TryGetValue("lives", out lives);

                    Console.WriteLine("\nCurrent Score: {0}\nRemaining Lives:{1}", score, lives);
                    Console.WriteLine("\n#######################################################\n");



                }
                else if (pickNumber > randomVar)
                {

                    GGString.TryGetValue("supMessage", out printValue2);
                    Console.WriteLine("\nThe output is {0}", printValue2);

                    GGInt["score"] -= 10;
                    GGInt["lives"] -= 1;

                    GGInt.TryGetValue("score", out score);
                    GGInt.TryGetValue("lives", out lives);

                    Console.WriteLine("\nCurrent Score: {0}\nRemaining Lives:{1}", score, lives);
                    Console.WriteLine("\n#######################################################\n");



                }

                else if (pickNumber == randomVar)
                {


                    GGString.TryGetValue("winMessage", out printValue3);
                    Console.WriteLine("\nThe output is {0}", printValue3);

                    GGInt.TryGetValue("score", out score);
                    GGInt.TryGetValue("lives", out lives);

                    Console.WriteLine("\nCurrent Score: {0}\nRemaining Lives:{1}", score, lives);
                    Console.WriteLine("\n#######################################################\n");

                    // Play again message
                    String play;
                    GGString.TryGetValue("playAgain", out play);
                    Console.WriteLine("\n" + play);

                    Console.WriteLine("\nWould you like to play again? Y/N\n");
                    playAgain = Console.ReadLine();
                    if (playAgain == "Y" || playAgain == "y")
                        marker = 0;
                    else
                        marker = 1;



                    return (marker);


                }

                else
                {
                    Console.WriteLine("Wrong Input");
                }


            }//while


            String failedMessage;
            GGString.TryGetValue("failMessage", out failedMessage);
            Console.WriteLine("\n*************************" + failedMessage + "*************************");
            Console.ReadLine();
            return (1);




        }


    }

}