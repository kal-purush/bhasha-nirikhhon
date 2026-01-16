using System.Drawing;

namespace AdventOfCode.Days;

public class Day09 : BaseDay
{
    private readonly List<(string Direction, int Steps)> _input = new();
    private readonly Dictionary<Point, int> _tailLocations = new Dictionary<Point, int>()
        {
            { new Point { X = 0, Y = 0 }, 1 }
        };

    public Day09()
    {
        _input = File.ReadAllLines(InputFilePath)
            .Select(x => x.Split(" "))
            .Select(x => (x[0], int.Parse(x[1])))
            .ToList();
    }

    public override ValueTask<string> Solve_1()
    {
        var head = new Point { X = 0, Y = 0 };
        var tail = new Point { X = 0, Y = 0 };

        foreach (var instruction in _input)
        {
            var steps = instruction.Steps;

            while (steps > 0)
            {
                switch (instruction.Direction)
                {
                    case "R":
                        head.X++;
                        break;
                    case "L":
                        head.X--;
                        break;
                    case "U":
                        head.Y++;
                        break;
                    case "D":
                        head.Y--;
                        break;
                }

                tail = MovePoint(head, tail);

                if (!_tailLocations.ContainsKey(tail))
                {
                    _tailLocations.Add(tail, 1);
                }

                steps--;
            }
        }

        return new(_tailLocations.Count.ToString());
    }

    public override ValueTask<string> Solve_2()
    {
        _tailLocations.Clear();

        var ropePoints = Enumerable.Range(0, 10).Select(x => new Point { X = 0, Y = 0 }).ToArray();

        foreach (var instruction in _input)
        {
            var steps = instruction.Steps;

            while (steps > 0)
            {
                var head = ropePoints[0];

                switch (instruction.Direction)
                {
                    case "R":
                        ropePoints[0].X++;
                        break;
                    case "L":
                        ropePoints[0].X--;
                        break;
                    case "U":
                        ropePoints[0].Y++;
                        break;
                    case "D":
                        ropePoints[0].Y--;
                        break;
                }

                for (var i = 1; i < ropePoints.Length; i++)
                {
                    ropePoints[i] = MovePoint(ropePoints[i - 1], ropePoints[i]);

                    if (i == ropePoints.Length - 1)
                    {
                        var tail = ropePoints[i];
                        if (!_tailLocations.ContainsKey(tail))
                        {
                            _tailLocations.Add(tail, 1);
                        }
                    }
                }

                steps--;
            }
        }

        return new(_tailLocations.Count.ToString());
    }

    private Point MovePoint(Point head, Point tail)
    {
        if (!IsTailTouchingHead(head, tail))
        {
            if (head.Y == tail.Y)
            {
                tail.X += head.X > tail.X ? 1 : -1;
            }
            else if (head.X == tail.X)
            {
                tail.Y += head.Y > tail.Y ? 1 : -1;
            }
            else
            {
                tail.Y += head.Y > tail.Y ? 1 : -1;
                tail.X += head.X > tail.X ? 1 : -1;
            }
        }

        return tail;
    }

    private bool IsTailTouchingHead(Point head, Point tail)
    {
        for (var x = tail.X - 1; x <= tail.X + 1; x++)
        {
            for (var y = tail.Y - 1; y <= tail.Y + 1; y++)
            {
                if (x == head.X && y == head.Y)
                    return true;
            }
        }

        return false;
    }
}