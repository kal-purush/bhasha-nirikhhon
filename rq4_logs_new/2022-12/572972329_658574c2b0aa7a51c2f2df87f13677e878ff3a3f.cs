namespace AdventOfCode.Days;

public class Day02 : BaseDay
{
    private readonly List<char[]> _input;
    private readonly Dictionary<char, int> _moveScore = new Dictionary<char, int>
    {
        { 'X', 1 },
        { 'Y', 2 },
        { 'Z', 3 }
    };

    public Day02()
    {
        _input = File.ReadAllLines(InputFilePath)
            .Select(x => x.Split(" ").Select(char.Parse).ToArray())
            .ToList();
    }

    public override ValueTask<string> Solve_1()
    {
        var opponentMapping = new Dictionary<char, char>
        {
            { 'X', 'A' },
            { 'Y', 'B' },
            { 'Z', 'C' }
        };

        var winningMove = new Dictionary<char, char>
        {
            { 'X', 'C' },
            { 'Y', 'A' },
            { 'Z', 'B' }
        };

        var score = 0;
        foreach (var move in _input)
        {
            if (move[0] == winningMove[move[1]])
            {
                score += 6;
            }
            else if (move[0] == opponentMapping[move[1]])
            {
                score += 3;
            }

            score += _moveScore[move[1]];
        }

        return new(score.ToString());
    }

    public override ValueTask<string> Solve_2()
    {
        var moveToWin = new Dictionary<char, char>
        {
            { 'C', 'X' },
            { 'A', 'Y' },
            { 'B', 'Z' }
        };

        var moveToLose = new Dictionary<char, char>
        {
            { 'B', 'X' },
            { 'C', 'Y' },
            { 'A', 'Z' }
        };

        var moveToDraw = new Dictionary<char, char>
        {
            { 'A', 'X' },
            { 'B', 'Y' },
            { 'C', 'Z' }
        };

        var score = 0;
        foreach (var move in _input)
        {
            char moveRequired;
            if (move[1] == 'Y')
            {
                score += 3;
                moveRequired = moveToDraw[move[0]];
            }
            else if (move[1] == 'Z')
            {
                score += 6;
                moveRequired = moveToWin[move[0]];
            }
            else
            {
                moveRequired = moveToLose[move[0]];
            }

            score += _moveScore[moveRequired];
        }

        return new(score.ToString());
    }

}