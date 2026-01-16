using System.Text.RegularExpressions;

namespace _2022.Days;

public class Day4 : Day
{
    private int _numOverlaps = 0;
    private int _numTotalOverlaps = 0;
    private readonly Regex LineMatch = new Regex("^([0-9]+)-([0-9]+),([0-9]+)-([0-9]+)$");
    
    public Day4() : base(4)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        var match = this.LineMatch.Match(line);

        if (match.Groups.Count != 5)
        {
            throw new ArgumentException($"Bad input detected, failed to parse: {line}", nameof(line));
        }

        var elf1Start = int.Parse(match.Groups[1].Captures.First().Value);
        var elf1End = int.Parse(match.Groups[2].Captures.First().Value);
        var elf2Start = int.Parse(match.Groups[3].Captures.First().Value);
        var elf2End = int.Parse(match.Groups[4].Captures.First().Value);

        if (elf1Start >= elf2Start && elf1End <= elf2End)
        {
            // Elf 1 within elf 2
            this._numTotalOverlaps++;
            this._numOverlaps++;
        }
        else if (elf2Start >= elf1Start && elf2End <= elf1End)
        {
            // Elf 2 within elf 1
            this._numTotalOverlaps++;
            this._numOverlaps++;
        }
        else if (elf1Start <= elf2End && elf1End >= elf2Start)
        {
            this._numOverlaps++;
        }
    }

    public override void SolvePart1()
    {
        this.Part1Solution = this._numTotalOverlaps.ToString();
    }

    public override void SolvePart2()
    {
        this.Part2Solution = this._numOverlaps.ToString();
    }
}