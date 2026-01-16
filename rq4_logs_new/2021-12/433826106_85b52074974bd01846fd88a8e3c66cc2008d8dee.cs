var input = File.ReadAllText("input.txt").Split(',').Select(c => int.Parse(c))
    .GroupBy(x => x).Select(g => (X: g.Key, C: g.Count())).OrderBy(x => x.X).ToArray();

Console.WriteLine($"Result A: {FindBest(input, CalculateCostA, (Position: 0, Cost: int.MaxValue))}");
Console.WriteLine($"Result B: {FindBest(input, CalculateCostB, (Position: 0, Cost: int.MaxValue))}");

static int FindBest((int X, int C)[] input, Func<int, (int X, int C), int> calculateCost, (int Position, int Cost) best)
{    
    for (int i = input.Min(x=> x.X); i <= input.Max(x => x.X); i++)
    {
        var cost = 0;
        foreach (var c in input)
            cost += calculateCost(i, c);
        
        if(cost > best.Cost)
            return best.Cost;

        best = (i, cost);
    }
    return best.Cost;
}

static int CalculateCostA(int i, (int X, int C) c) => Math.Abs((i - c.X) * c.C);
static int CalculateCostB(int i, (int X, int C) c) => SumS(Math.Abs(i - c.X)) * c.C;
static int SumS(int n) => (n * (n + 1) / 2);