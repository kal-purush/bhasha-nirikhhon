using System.Globalization;
using System.Text.RegularExpressions;

namespace _2022.Days;

public partial class Day21 : Day
{
    private readonly Dictionary<string, Monkey> _monkeys = new ();
    
    public Day21() : base(21)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        var monkey = new Monkey(line, this._monkeys);
        
        this._monkeys.Add(monkey.Id, monkey);
    }

    protected override void SolvePart1()
    {
        var rootMonkey = this._monkeys["root"];

        this.Part1Solution = rootMonkey.GetResult().ToString();
    }

    protected override void SolvePart2()
    {
        var rootMonkey = this._monkeys["root"];

        this.Part2Solution = rootMonkey.GetHumanNumber().ToString();
    }

    private partial class Monkey
    {
        private static readonly Regex LineRegex = MyRegex();
        
        public string Id { get; }
        private readonly IReadOnlyDictionary<string, Monkey> _monkeys;

        private readonly long? _value;

        private readonly char _op;
        private readonly string _leftMonkeyId;
        private readonly string _rightMonkeyId;

        private Monkey LeftMonkey => this._monkeys[this._leftMonkeyId];
        private Monkey RightMonkey => this._monkeys[this._rightMonkeyId];

        private long? _cachedResult;

        public Monkey(string line, IReadOnlyDictionary<string, Monkey> monkeys)
        {
            this._monkeys = monkeys;
            
            var match = LineRegex.Match(line);

            if (match.Success is false)
                throw new ArgumentException($"Invalid monkey {line}");

            this.Id = match.Groups[1].Value;

            if (match.Groups[2].Success)
            {
                // Number
                this._value = long.Parse(match.Groups[2].Value);

                this._op = ' ';
                this._leftMonkeyId = "";
                this._rightMonkeyId = "";
            }
            else
            {
                // 2 other monkeys
                this._leftMonkeyId = match.Groups[3].Value;
                this._rightMonkeyId = match.Groups[5].Value;

                this._op = match.Groups[4].Value[0];
            }
        }

        public long GetResult()
        {
            if (this._value.HasValue)
            {
                return this._value.Value;
            }
            else
            {
                var leftMonkeyValue = this.LeftMonkey.GetResult();
                var rightMonkeyValue = this.RightMonkey.GetResult();

                var calculatedValue = this._op switch
                {
                    '+' => leftMonkeyValue + rightMonkeyValue,
                    '-' => leftMonkeyValue - rightMonkeyValue,
                    '*' => leftMonkeyValue * rightMonkeyValue,
                    '/' => leftMonkeyValue / rightMonkeyValue,
                    _ => throw new ApplicationException($"Invalid operator {this._op}")
                };

                this._cachedResult = calculatedValue;

                return calculatedValue;
            }
        }

        public long GetHumanNumber()
        {
            if (this.LeftMonkey.IsHumanControlled(this.RightMonkey._cachedResult.Value, out var humanNumber))
            {
                return humanNumber;
            }
            else if (this.RightMonkey.IsHumanControlled(this.LeftMonkey._cachedResult.Value, out humanNumber))
            {
                return humanNumber;
            }
            else
            {
                throw new ApplicationException("Failed to find human number");
            }
        }

        private bool IsHumanControlled(long shouldEqual, out long humanNumber)
        {
            if (this.Id is "humn")
            {
                humanNumber = shouldEqual;
                return true;
            }
            else if (this._value.HasValue)
            {
                humanNumber = -1;
                return false;
            }
            else
            {
                long leftMonkeyShouldEqual;
                long rightMonkeyShouldEqual;
                
                switch (this._op)
                {
                    case '+':
                        leftMonkeyShouldEqual = shouldEqual - this.RightMonkey.GetCachedValueOrValue();
                        rightMonkeyShouldEqual = shouldEqual - this.LeftMonkey.GetCachedValueOrValue();
                        break;
                    case '-':
                        leftMonkeyShouldEqual = shouldEqual + this.RightMonkey.GetCachedValueOrValue();
                        rightMonkeyShouldEqual = this.LeftMonkey.GetCachedValueOrValue() - shouldEqual;
                        break;
                    case '*':
                        leftMonkeyShouldEqual = shouldEqual / this.RightMonkey.GetCachedValueOrValue();
                        rightMonkeyShouldEqual = shouldEqual / this.LeftMonkey.GetCachedValueOrValue();
                        break;
                    case '/':
                        leftMonkeyShouldEqual = shouldEqual * this.RightMonkey.GetCachedValueOrValue();
                        rightMonkeyShouldEqual = this.LeftMonkey.GetCachedValueOrValue() / shouldEqual;
                        break;
                    default:
                        throw new ApplicationException($"Unexpected op: {this._op}");
                }

                return this.LeftMonkey.IsHumanControlled(leftMonkeyShouldEqual, out humanNumber) || this.RightMonkey.IsHumanControlled(rightMonkeyShouldEqual, out humanNumber);
            }
        }

        private long GetCachedValueOrValue()
        {
            if (this._value.HasValue)
            {
                return this._value.Value;
            }
            else if (this._cachedResult.HasValue)
            {
                return this._cachedResult.Value;
            }
            else
            {
                throw new ApplicationException($"Monkey {this.Id} has neither value nor cached value!");
            }
        }

        [GeneratedRegex("^([a-z]+): (?:([0-9]+)|(?:([a-z]+) ([+\\-*\\/]) ([a-z]+)))$")]
        private static partial Regex MyRegex();
    }
}