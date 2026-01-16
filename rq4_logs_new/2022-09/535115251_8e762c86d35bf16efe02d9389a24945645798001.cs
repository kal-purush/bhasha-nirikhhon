using System;

namespace MathGame.Game
{
    class Game
    {
        int startCount, totalNumbers;
        List<Rules> rules;

        public Game(
            int startCount, 
            int totalNumbers, 
            List<Rules> rules
        ){
            this.startCount = startCount;
            this.totalNumbers = totalNumbers;
            this.rules = rules;
        }

        public void RunGame()
        {
            for (int i = startCount; i <= totalNumbers; i++)
            {
                var output = "";

                foreach (var rule in rules) 
                {
                    if (CheckRule(i, rule.number))
                    {
                        output += rule.name;
                    }
                }

                if (output.Length == 0)
                    Console.WriteLine(i);
                else
                    Console.WriteLine(output);
            }
        }

        private bool CheckRule(int number, int numberToCheckAgainst)
        {
            if (number % numberToCheckAgainst == 0)
            {
                return true;
            }

            return false;
        }
    }
}