using System.Globalization;

namespace _2022.Days;

public class Day11 : Day
{
    private static readonly List<Monkey> Monkeys = new();

    private PartialMonkey? _partialMonkey;

    public Day11() : base(11)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        if (line.Length is 0)
        {
            if (this._partialMonkey is null)
                throw new ArgumentException("Got empty line, but no partial monkey");

            if (this._partialMonkey.IsComplete() is false)
                throw new ArgumentException("Got empty line, but partial monkey is not complete");
            
            Monkeys.Add(this._partialMonkey.GetMonkey());

            this._partialMonkey = null;

            return;
        }
        
        var parts = line.Trim().Split(':');

        this._partialMonkey ??= new PartialMonkey();

        switch (parts[0][0])
        {
            case 'M':
                // Monkey
                this._partialMonkey.Num = int.Parse(parts[0].Last().ToString());
                break;
            case 'S':
                // Starting items
                var items = parts[1].Trim().Split(", ").Select(int.Parse);
                this._partialMonkey.Items = new List<int>(items);
                break;
            case 'O':
                // Operation
                this._partialMonkey.Operation = ParseOperation(parts[1].Trim());
                break;
            case 'T':
                // Test
                this._partialMonkey.TestDivisor = int.Parse(parts[1].Split(' ').Last());
                break;
            case 'I':
                // If true/false
                if (parts[0] is "If true")
                {
                    this._partialMonkey.TruthyMonkey = int.Parse(parts[1].Split(' ').Last());
                }
                else
                {
                    this._partialMonkey.FalsyMonkey = int.Parse(parts[1].Split(' ').Last());
                }
                break;
            default:
                throw new ArgumentException($"Invalid line {line}");
        }
    }

    private static Func<int, int> ParseOperation(string operation)
    {
        // String in format new = (old | num) op (old | num)
        var parts = operation.Split(" = ");

        var operationParts = parts[1].Split(' ');

        return old =>
        {
            var lhs = operationParts[0] is "old" ? old : int.Parse(operationParts[0]);
            var rhs = operationParts[2] is "old" ? old : int.Parse(operationParts[2]);

            return operationParts[1] switch
            {
                "+" => lhs + rhs,
                "-" => lhs - rhs,
                "*" => lhs * rhs,
                _ => throw new ArgumentException($"Unsupported operation: {operationParts[1]}")
            };
        };
    }

    public override void SolvePart1()
    {
        if (this._partialMonkey is not null)
        {
            if (this._partialMonkey.IsComplete() is false)
                throw new ArgumentException("Input processing finished, but incomplete partial monkey found");
            
            Monkeys.Add(this._partialMonkey.GetMonkey());
        }

        for (var i = 1; i <= 20; i++)
        {
            // Round i
            foreach (var monkey in Monkeys)
            {
                monkey.DoInspections();
            }
        }

        this.Part1Solution = GetMonkeyBusiness();
    }

    public override void SolvePart2()
    {
        foreach (var monkey in Monkeys)
        {
            monkey.InitialiseModuloItems(Monkeys.Count);
            monkey.ResetNumInspections();
        }
        
        for (var i = 1; i <= 10000; i++)
        {
            foreach (var monkey in Monkeys)
            {
                monkey.DoModuloInspections();
            }
        }
        
        this.Part2Solution = GetMonkeyBusiness();
    }

    private static string GetMonkeyBusiness()
    {
        return Monkeys
            .Select(m => m.GetNumInspections())
            .OrderDescending()
            .Take(2)
            .Aggregate(1d, (acc, numInspections) => acc * numInspections)
            .ToString(CultureInfo.InvariantCulture);
    }

    private class PartialMonkey
    {
        public int? Num;
        public List<int>? Items;
        public Func<int, int>? Operation;
        public int? TestDivisor;
        public int? TruthyMonkey;
        public int? FalsyMonkey;

        public bool IsComplete()
        {
            return this.Num is not null &&
                   this.Items is not null &&
                   this.Operation is not null &&
                   this.TestDivisor is not null &&
                   this.TruthyMonkey is not null &&
                   this.FalsyMonkey is not null;
        }

        public Monkey GetMonkey()
        {
            if (!this.IsComplete())
                throw new ArgumentException("Tried to get monkey, but not all fields initialised!");

            return new Monkey(
                this.Num!.Value,
                this.Items!,
                this.Operation!, 
                this.TestDivisor!.Value,
                this.TruthyMonkey!.Value,
                this.FalsyMonkey!.Value);
        }
    }

    private class Monkey
    {
        private readonly int _num;
        private readonly List<int> _initialItems;
        private readonly List<int> _items;
        private List<List<int>>? _moduloItems;
        private readonly Func<int, int> _operation;
        private readonly int _testDivisor;
        private readonly int _truthyMonkey;
        private readonly int _falsyMonkey;

        private int _numInspections;

        public Monkey(int num, List<int> items, Func<int, int> operation, int testDivisor, int truthyMonkey, int falsyMonkey)
        {
            this._num = num;
            this._items = items;
            this._initialItems = new List<int>(items);
            this._operation = operation;
            this._testDivisor = testDivisor;
            this._truthyMonkey = truthyMonkey;
            this._falsyMonkey = falsyMonkey;
        }

        public void InitialiseModuloItems(int numMonkeys)
        {
            this._moduloItems = this._initialItems.Select(item =>
            {
                var newItems = new List<int>();

                for (var i = 0; i < numMonkeys; i++)
                {
                    newItems.Add(item);
                }

                return newItems;
            }).ToList();
        }

        public void ResetNumInspections()
        {
            this._numInspections = 0;
        }

        public void DoInspections()
        {
            foreach (var item in this._items)
            {
                this._numInspections++;
                
                var worryLevel = this._operation(item);
                var worryLevelAfterBoredom = (int) Math.Floor(worryLevel / 3d);

                var targetMonkey = worryLevelAfterBoredom % this._testDivisor == 0 ? Monkeys[this._truthyMonkey] : Monkeys[this._falsyMonkey];
                
                targetMonkey.AddItem(worryLevelAfterBoredom);
            }
            
            this._items.Clear();
        }

        public void DoModuloInspections()
        {
            foreach (var moduloItems in this._moduloItems)
            {
                this._numInspections++;

                var newModuloItems = new List<int>();
                Monkey targetMonkey = null;

                for (var i = 0; i < Monkeys.Count; i++)
                {
                    var item = moduloItems[i];
                    var modulo = Monkeys[i]._testDivisor;

                    var worryLevel = this._operation(item) % modulo;
                    newModuloItems.Add(worryLevel);

                    if (i == this._num)
                    {
                        targetMonkey = worryLevel is 0 ? Monkeys[this._truthyMonkey] : Monkeys[this._falsyMonkey];
                    }
                }
                
                targetMonkey.AddModuloItem(newModuloItems);
            }
            
            this._moduloItems.Clear();
        }
        
        private void AddItem(int item)
        {
            this._items.Add(item);
        }

        private void AddModuloItem(List<int> moduloItems)
        {
            this._moduloItems.Add(moduloItems);
        }

        public int GetNum()
        {
            return this._num;
        }

        public int GetNumInspections()
        {
            return this._numInspections;
        }
    }
}