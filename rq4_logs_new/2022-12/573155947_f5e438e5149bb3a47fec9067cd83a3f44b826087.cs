namespace AdventOfCode;

public class Day02 : BaseDay
{
    private readonly string _input;

    public Day02()
    {
        _input = File.ReadAllText(InputFilePath);
    }

    private readonly HashSet<Game> Player1Wins = new HashSet<Game>()
    {
        new(Action.Rock, Action.Scissor),
        new(Action.Scissor, Action.Paper),
        new(Action.Paper, Action.Rock),
    };

    public override ValueTask<string> Solve_1() => new(_input.SplitNewLine().Select(x =>
    {
        var values = x.Split(" ");
        return new Game(StringToAction(values[0]), StringToAction(values[1]));
    }).Select(x =>
    {
        if (x.Player1 == x.Player2)
        {
            return (int)x.Player2 + 3;
        }
        else if (Player1Wins.Contains(x))
        {
            //P1 Win
            return (int)x.Player2;
        }
        else
        {
            //P2 win
            return (int)x.Player2 + 6;
        }
    }).Sum().ToString());

    public override ValueTask<string> Solve_2() => new("");

    private Action StringToAction(string character) => character switch
    {
        "A" => Action.Rock,
        "B" => Action.Paper,
        "C" => Action.Scissor,

        "X" => Action.Rock,
        "Y" => Action.Paper,
        "Z" => Action.Scissor,
    };
}

public enum Action
{
    Rock = 1,
    Paper = 2,
    Scissor = 3,
}

public record Game(Action Player1, Action Player2);