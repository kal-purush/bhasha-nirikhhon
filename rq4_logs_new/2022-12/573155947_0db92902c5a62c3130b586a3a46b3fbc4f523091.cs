namespace AdventOfCode.Day09;

public class Day09 : Day
{
    public Day09()
    {
        ParsedInput = ParseInput(_input);
    }
    
    public Action[] ParsedInput { get; }

    private Action[] ParseInput(string input)
        => input.SplitNewLine().Select(Action.Parse).ToArray();

    public override ValueTask<string> Solve_1()
        => new(PlayBoard(new Board(new Point(0, 0), new Point(0, 0)), ParsedInput).seenPositions.Count.ToString());

    private (Board board, HashSet<Point> seenPositions) PlayBoard(Board board, Action[] actions)
    {
        HashSet<Point> seenPositions = new()
        {
            new (0,0)
        };
        foreach (var action in actions)
        {
            for (var i = 1; i <= action.Amount; i++)
            {
                // Move H
                board.PosH = action.Direction switch
                {
                    Direction.Down => board.PosH with { Y = board.PosH.Y - 1, },
                    Direction.Up => board.PosH with { Y = board.PosH.Y + 1, },
                    Direction.Left => board.PosH with { X = board.PosH.X - 1, },
                    Direction.Right => board.PosH with { X = board.PosH.X + 1, },
                };

                // Move T
                var difference = Difference(board.PosH, board.PosT);
                

                if (Math.Abs(difference.Y) <= 1 && Math.Abs(difference.X) <= 1)
                {
                    continue;
                }
                if (difference.X == 0 && Math.Abs(difference.Y) > 1)
                {
                    board.PosT = board.PosT with { Y = board.PosT.Y + Math.Sign(difference.Y), };
                }
                else if (difference.Y == 0 && Math.Abs(difference.X) > 1)
                {
                    board.PosT = board.PosT with { X = board.PosT.X + Math.Sign(difference.X), };
                }
                else
                {
                    board.PosT = board.PosT with { X = board.PosT.X + Math.Sign(difference.X), Y = board.PosT.Y + Math.Sign(difference.Y), };
                }


                seenPositions.Add(board.PosT);
            }
        }

        return (board, seenPositions);
    }

    public override ValueTask<string> Solve_2()
        => throw new NotImplementedException();

    public static Point Difference(Point PosH, Point PoshT)
        => new(PosH.X - PoshT.X, PosH.Y - PoshT.Y);
}

public class Board
{
    public Board(Point PosH, Point PosT)
    {
        this.PosH = PosH;
        this.PosT = PosT;
    }

    public Point PosH { get; set; }
    public Point PosT { get; set; }

    public void Deconstruct(out Point PosH, out Point PosT)
    {
        PosH = this.PosH;
        PosT = this.PosT;
    }
}

public record struct Point(int X, int Y);

public enum Direction
{
    Up,
    Down,
    Left,
    Right,
}

public record Action(Direction Direction, long Amount)
{
    public static Action Parse(string input)
    {
        var split = input.SplitSpace();
        return split[0] switch
        {
            "U" => new Action(Direction.Up, split[1].AsLong()),
            "D" => new Action(Direction.Down, split[1].AsLong()),
            "L" => new Action(Direction.Left, split[1].AsLong()),
            "R" => new Action(Direction.Right, split[1].AsLong()),
        };
    }
}