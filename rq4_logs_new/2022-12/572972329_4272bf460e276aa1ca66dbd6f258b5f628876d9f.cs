namespace AdventOfCode.Days;

public class Day04 : BaseDay
{
    private readonly int[][] _input;

    public Day04()
    {
        _input = File.ReadAllLines(InputFilePath)
            .Select(x => x.Replace("-", ",")
                        .Split(",")
                        .Select(int.Parse)
                        .ToArray())
            .ToArray();
    }

    public override ValueTask<string> Solve_1()
    {
        var overlapCount = 0;
        foreach (var sectionPairs in _input)
        {
            var firstRange = string.Join(",", Enumerable.Range(sectionPairs[0], sectionPairs[1] - sectionPairs[0] + 1).Select(x => x.ToString().PadLeft(2, '0')));
            var secondRange = string.Join(",", Enumerable.Range(sectionPairs[2], sectionPairs[3] - sectionPairs[2] + 1).Select(x => x.ToString().PadLeft(2, '0')));

            if (firstRange.Contains(secondRange) || secondRange.Contains(firstRange))
                overlapCount++;
        }

        return new(overlapCount.ToString());
    }

    public override ValueTask<string> Solve_2()
    {
        var overlapCount = 0;
        foreach (var sectionPairs in _input)
        {
            var firstRange = string.Join(",", Enumerable.Range(sectionPairs[0], sectionPairs[1] - sectionPairs[0] + 1).Select(x => x.ToString().PadLeft(2, '0')));
            var secondRange = string.Join(",", Enumerable.Range(sectionPairs[2], sectionPairs[3] - sectionPairs[2] + 1).Select(x => x.ToString().PadLeft(2, '0')));

            foreach (var number in firstRange.Split(","))
            {
                if (secondRange.Contains(number))
                {
                    overlapCount++;
                    break;
                }
            }
        }

        return new(overlapCount.ToString());
    }
}