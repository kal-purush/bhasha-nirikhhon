namespace AdventOfCode;

public class Day03 : BaseDay
{
    private readonly string _input;

    public Day03()
    {
        _input = File.ReadAllText(InputFilePath);
    }

    public override ValueTask<string> Solve_1() => new(_input.SplitNewLine().Select((x =>
    {
        var input = x.ToCharArray();
        return new Bag(input[0..(input.Length / 2)], input[(input.Length / 2)..]);
    })).Select(GetChar).Select(GetScore).Sum().ToString());

    private double GetScore(char arg)
        => char.IsUpper(arg)
            ? arg - 64  + 26
            : arg - 96;

    private char GetChar(Bag bag)
    {
        var compartment1 = new HashSet<char>(bag.Compartment1);
        compartment1.IntersectWith(bag.Compartment2);
        return compartment1.First();
    }

    public override ValueTask<string> Solve_2() => new("");
}

public record Bag(char[] Compartment1, char[] Compartment2);