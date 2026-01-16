using System.Collections.Immutable;
using System.Text;

namespace _2022.Days;

public class Day10 : Day
{
    private int _cycle = 1;
    private int _x = 1;
    private int _signalStrengthSum = 0;

    private StringBuilder _display = new();

    private readonly ImmutableHashSet<int> _interestingCycles = new HashSet<int>
    {
        20,
        60,
        100,
        140,
        180,
        220
    }.ToImmutableHashSet();
    
    public Day10() : base(10)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        var parts = line.Split(' ');

        if (parts.Length == 0)
            throw new ArgumentException("Got empty line!");

        var cmd = parts[0];

        switch (cmd)
        {
            case "noop":
                this.CheckAndIncrementCycle();
                break;
            case "addx":
                if (parts.Length != 2)
                    throw new ArgumentException("Processing addx but got no value");
                
                this.ProcessAddx(parts[1]);
                break;
            default:
                throw new ArgumentException($"Unexpected command {cmd}");
        }
    }

    private void ProcessAddx(string valueStr)
    {
        if (!int.TryParse(valueStr, out var value))
            throw new ArgumentException($"Expected {valueStr} to be an integer");
        
        this.CheckAndIncrementCycle();
        this.CheckAndIncrementCycle();

        this._x += value;
    }

    private void CheckAndIncrementCycle()
    {
        if (this._interestingCycles.Contains(this._cycle))
        {
            this._signalStrengthSum += this._cycle * this._x;
        }

        var pixel = (this._cycle - 1) % 40;

        this._display.Append(Math.Abs(pixel - this._x) <= 1 ? '#' : '.');

        if (pixel is 39)
        {
            this._display.AppendLine();
        }

        this._cycle++;
    }

    public override void SolvePart1()
    {
        this.Part1Solution = this._signalStrengthSum.ToString();
    }

    public override void SolvePart2()
    {
        this.Part2Solution = this._display.ToString();
    }
}