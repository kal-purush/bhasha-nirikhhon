using System;

namespace IfTernatyChallange;
class Program
{
    static int highscore = 300;
    static string highscorePlayer = "Denis";

    static void Main(string[] args)
    {
        CheckHighscore(250, "Maria");
        CheckHighscore(315, "Michael");
        CheckHighscore(350, "Kevin aka KÉKÉ");

        Console.WriteLine("\n - - - - -\n");

        int temperature = 0;
        string stateOfMatter;

        if (temperature <= 0) stateOfMatter = "solid";
        else stateOfMatter = "liquid";

        Console.WriteLine("State of matter is {0}", stateOfMatter);

        temperature += 30;
        // Ternary :
        stateOfMatter = temperature < 0 ? "solid" : "liquid";

        Console.WriteLine("State of matter is {0}", stateOfMatter);

        temperature += 100;
        stateOfMatter = temperature > 100 ? "gas"
                        : temperature < 0 ? "solid" : "liquid";

        Console.WriteLine("State of matter is {0}", stateOfMatter);
    }

    public static void CheckHighscore(int score, string playerName)
    {
        Console.WriteLine(" - - - - -\nPlayer {0} is playing.\n", playerName);

        if (score > highscore)
        {
            highscore = score;
            highscorePlayer = playerName;

            Console.WriteLine("New highscore is {0},\n Held by {1}.", score, playerName);
        }
        else
        {
            Console.WriteLine("Highscore is not reach,highscore is {0},\n Held by {1}.", highscore, highscorePlayer);
        }
    }
}