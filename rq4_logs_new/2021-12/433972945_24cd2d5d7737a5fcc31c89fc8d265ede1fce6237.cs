namespace AocNetLib
{
    public class Day7
    {
        public string Solve(string input)
        {
            var positions = ParseInput(input);
            int cost = Optimize(positions, GetCost);
            return cost.ToString();
        }
        public string Solve2(string input)
        {
            var positions = ParseInput(input);
            int cost = Optimize(positions, GetCost2);
            return cost.ToString();
        }

        private int Optimize(int[] positions, Func<int[],int,int> costFunction)
        {
            int cost;
            int target = (int)positions.Average();
            cost = costFunction(positions, target);
            bool isOptimized;
            // check upwards
            do
            {
                isOptimized = false;
                int costOpt = costFunction(positions, target + 1);
                if (costOpt < cost)
                {
                    cost = costOpt;
                    target++;
                    isOptimized = true;
                }
            } while (isOptimized);
            // check downwards
            do
            {
                isOptimized = false;
                int costOpt = costFunction(positions, target - 1);
                if (costOpt < cost)
                {
                    cost = costOpt;
                    target--;
                    isOptimized = true;
                }
            } while (isOptimized);
            Console.WriteLine($"Target: {target}");
            return cost;
        }

        int GetCost(int[] positions, int target)
        {
            return positions.Sum(x => Math.Abs(x - target));
        }

        int GetCost2(int[] positions, int target)
        {
            return positions.Sum(x => GetFuel2(target, x));
        }

        private static int GetFuel2(int target, int pos)
        {
            int distance = Math.Abs(pos - target);
            int fuel = 0;
            for (int i = 1; i <= distance; i++) fuel += i;
            return fuel;
        }

        static int[] ParseInput(string input)
        {
            return input.Split(',').Select(x=>int.Parse(x)).ToArray();
        }
    }
}