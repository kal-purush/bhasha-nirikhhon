namespace AdventOfCode;

public class Day04 : BaseDay
{
    private readonly string _input;

    public Day04()
    {
        _input = File.ReadAllText(InputFilePath);
    }

    public override ValueTask<string> Solve_1()
        => new(_input.SplitNewLine().Select(x => x.Split(',').Select(c =>
        {
            var b =  c.Split('-').Select(x => x.AsLong()).ToArray();
            return new Range(b[0], b[1]);
        }).ToArray()).Count(IsOverLap).ToString());

    private bool IsOverLap(Range[] lines)
    {
        var line1 = lines[0];
        var line2 = lines[1];

        return line1.Start >= line2.Start && line1.End <= line2.End ||
               line1.Start <= line2.Start && line1.End >= line2.End;
    }

    public override ValueTask<string> Solve_2()
        => new("");
}

public record Range(long Start, long End);