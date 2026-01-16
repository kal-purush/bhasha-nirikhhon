namespace AdventOfCode.Days;

public class Day03 : BaseDay
{
    private readonly string[] _input;

    public Day03()
    {
        _input = File.ReadAllLines(InputFilePath);
    }

    public override ValueTask<string> Solve_1()
    {
        var score = 0;
        foreach (var rucksack in _input)
        {
            var compartment1 = rucksack.Substring(0, rucksack.Length / 2);
            var compartment2 = rucksack.Substring(rucksack.Length / 2);

            var commonCharacter = compartment1.Intersect(compartment2).FirstOrDefault();

            score += char.IsUpper(commonCharacter)
                    ? commonCharacter - 38
                    : commonCharacter - 96;
        }

        return new(score.ToString());
    }

    public override ValueTask<string> Solve_2()
    {
        var score = 0;
        for (var rucksackIndex = 0; rucksackIndex < _input.Length; rucksackIndex += 3)
        {
            var rucksack1 = _input[rucksackIndex];
            var rucksack2 = _input[rucksackIndex + 1];
            var rucksack3 = _input[rucksackIndex + 2];

            var commonCharacter = rucksack1.Intersect(rucksack2).Intersect(rucksack3).FirstOrDefault();

            score += char.IsUpper(commonCharacter)
                    ? commonCharacter - 38
                    : commonCharacter - 96;
        }

        return new(score.ToString());
    }
}