namespace AdventOfCode.Days;

public class Day11 : BaseDay
{
    private readonly Dictionary<int, Monkey> _monkeys = new();
    private readonly List<Monkey> _input = new();
    private int _supermodulo = 1;

    public Day11()
    {
        var monkeyData = File.ReadAllText(InputFilePath)
            .Split($"{Environment.NewLine}{Environment.NewLine}")
            .Select(x => x.Split($"{Environment.NewLine}"))
            .Select(x => x.Select(y => y.Trim()).ToArray())
            .ToArray();
        
        CreateMonkeys(monkeyData);
    }
    
    public override ValueTask<string> Solve_1()
    {
        var round = 0;

        while (round < 20)
        {
            foreach (var monkey in _input.Where(monkey => monkey.Items.Count != 0))
            {
                while (monkey.Items.Count > 0)
                {
                    monkey.NumberOfInspections++;
                    var worryLevel = monkey.Items.Dequeue();
                    worryLevel = monkey.Operation(worryLevel);
                    worryLevel /= 3;

                    if (monkey.Test(worryLevel))
                    {
                        _monkeys[monkey.TrueMonkey].Items.Enqueue(worryLevel);
                    }
                    else
                    {
                        _monkeys[monkey.FalseMonkey].Items.Enqueue(worryLevel);
                    }
                }
            }

            round++;
        }

        var results = _input.Select(x => x.NumberOfInspections).OrderByDescending(x => x).ToArray();

        return new((results[0] * results[1]).ToString());
    }

    public override ValueTask<string> Solve_2()
    {
        ResetMonkeys();
        var round = 0;

        while (round < 10000)
        {
            foreach (var monkey in _input.Where(monkey => monkey.Items.Count != 0))
            {
                monkey.NumberOfInspections += monkey.Items.Count;
                
                while (monkey.Items.Count > 0)
                {
                    var worryLevel = monkey.Items.Dequeue();
                    worryLevel = monkey.Operation(worryLevel);
                    worryLevel %= _supermodulo;
                    
                    if (monkey.Test(worryLevel))
                    {
                        _monkeys[monkey.TrueMonkey].Items.Enqueue(worryLevel);
                    }
                    else
                    {
                        _monkeys[monkey.FalseMonkey].Items.Enqueue(worryLevel);
                    }
                }
            }

            round++;
        }

        var results = _input.Select(x => x.NumberOfInspections).OrderByDescending(x => x).ToArray();
        var result = (ulong)results[0] * (ulong)results[1];
        return new(result.ToString());
    }

    private void CreateMonkeys(string[][] monkeyData)
    {
        for (var i = 0; i < monkeyData.Length; i++)
        {
            var data = monkeyData[i];
            
            var items = new Queue<long>();
            foreach (var item in data[1][15..].Split(", ").Select(long.Parse))
            {
                items.Enqueue(item);
            }

            var value = data[2][23..] == "old" 
                ?  0
                : int.Parse(data[2][23..]);
            
            Func<long, long> operation = data[2][21] == '*'
                ? (x) => x * (value == 0 ? x : value)
                : (x) => x + (value == 0 ? x : value);

            var testValue = int.Parse(data[3][19..]);
            
            // I genuinely didn't have a clue how to do Part 2 and looking for help didn't lead me anywhere. I copied this supermodulo idea from 
            // https://old.reddit.com/r/adventofcode/comments/zih7gf/2022_day_11_part_2_what_does_it_mean_find_another/izr79go/
            // but I still have no idea how/why it works
            _supermodulo *= testValue;
            
            bool Test(long x) => x % testValue == 0;

            var monkey = new Monkey
            {
                Items = items,
                OriginalItems = items.ToList(),
                Operation = operation,
                Test = Test,
                TrueMonkey = int.Parse(data[4][25].ToString()),
                FalseMonkey = int.Parse(data[5][26].ToString())
            };
            
            _input.Add(monkey);
            _monkeys.Add(i, monkey);
        }
    }

    private void ResetMonkeys()
    {
        foreach (var monkey in _input)
        {
            monkey.Items = new Queue<long>();
            foreach (var item in monkey.OriginalItems)
            {
                monkey.Items.Enqueue(item);
            }

            monkey.NumberOfInspections = 0;
        }
    }
}

public class Monkey
{
    public Func<long, long> Operation { get; init; }
    public Func<long, bool> Test { get; init; }
    public Queue<long> Items { get; set; }
    public List<long> OriginalItems { get; init; }
    public int TrueMonkey { get; init; }
    public int FalseMonkey { get; init; }
    public int NumberOfInspections { get; set; }
}