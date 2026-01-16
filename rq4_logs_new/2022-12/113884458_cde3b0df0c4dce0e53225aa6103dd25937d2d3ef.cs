using System.Text;
using _2022.Utils;

namespace _2022.Days;

public class Day25 : Day
{
    private long _sum = 0;
    
    public Day25() : base(25)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        this._sum += line.Select(GetVal).Aggregate(0L, (acc, digit) => acc * 5 + digit);
    }

    private static long GetVal(char c)
    {
        return c switch
        {
            '=' => -2,
            '-' => -1,
            '0' => 0,
            '1' => 1,
            '2' => 2,
            _ => throw new ArgumentOutOfRangeException(nameof(c), $"Unexpected snafu char {c}")
        };
    }

    protected override void SolvePart1()
    {
        var snafuSum = new StringBuilder();

        while (this._sum != 0)
        {
            snafuSum.Insert(0, (((this._sum % 5) + 5) % 5) switch { 0 => '0', 1 => '1', 2 => '2', 3 => '=', 4 => '-' });
            this._sum = (this._sum - GetVal(snafuSum[0])) / 5;
        }

        this.Part1Solution = snafuSum.ToString();
    }

    protected override void SolvePart2()
    {
    }
}