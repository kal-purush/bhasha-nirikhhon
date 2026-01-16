using System.Drawing;

namespace _2022.Days;

public class Day9 : Day
{
    private readonly HashSet<Point> _pointsVisitedByKnot1 = new();
    private readonly HashSet<Point> _pointsVisitedByKnot9 = new();
    
    private Point _headPos = new(0, 0);
    private readonly Dictionary<int, Point> _knotPoses = new();

    public Day9() : base(9)
    {
        for (var i = 1; i <= 9; i++)
        {
            this._knotPoses.Add(i, new Point(0, 0));
        }
    }

    protected override void ProcessInputLine(string line)
    {
        var parts = line.Split(' ');

        if (parts.Length != 2)
            throw new ArgumentException($"Invalid format for line: {line}");

        var dir = parts[0];
        
        if (!int.TryParse(parts[1], out var num))
            throw new ArgumentException($"Expected {parts[1]} to be a number");

        this.DoSteps(dir, num);
    }

    private void DoSteps(string dir, int numSteps)
    {
        // Move the head
        var velocity = dir switch
        {
            "U" => new Point(0, 1),
            "D" => new Point(0, -1),
            "L" => new Point(-1, 0),
            "R" => new Point(1, 0),
            _ => throw new ArgumentException($"Invalid direction {dir}")
        };

        while (numSteps > 0)
        {
            this.DoSingleStep(velocity);
            numSteps--;
        }
    }

    private void DoSingleStep(Point velocity)
    {
        // Mark places as visited
        var knot1 = this._knotPoses[1];
        var knot9 = this._knotPoses[9];

        this._pointsVisitedByKnot1.Add(new Point(knot1.X, knot1.Y));
        this._pointsVisitedByKnot9.Add(new Point(knot9.X, knot9.Y));
        
        // Move the knots
        this._headPos.Offset(velocity);

        for (var i = 1; i <= 9; i++)
        {
            MakeKnotFollow(i - 1, i);
        }
    }

    private void MakeKnotFollow(int knotNumJustMoved, int knotNumToFollow)
    {
        var movingKnot = knotNumJustMoved == 0 ? this._headPos : this._knotPoses[knotNumJustMoved];
        var followingKnot = this._knotPoses[knotNumToFollow];
        
        // Update the tail
        var xDiff = movingKnot.X - followingKnot.X;
        var yDiff = movingKnot.Y - followingKnot.Y;

        if (Math.Abs(xDiff) != 2 && Math.Abs(yDiff) != 2) return;
        
        // Need to move the tail
        var tailVelocity = xDiff switch
        {
            -2 => yDiff switch
            {
                > 0 => new Point(-1, 1),
                0 => new Point(-1, 0),
                < 0 => new Point(-1, -1)
            },
            -1 => yDiff switch
            {
                2 => new Point(-1, 1),
                -2 => new Point(-1, -1),
                _ => new Point(0, 0)
            },
            0 => yDiff switch
            {
                2 => new Point(0, 1),
                -2 => new Point(0, -1),
                _ => new Point(0, 0)
            },
            1 => yDiff switch
            {
                2 => new Point(1, 1),
                -2 => new Point(1, -1),
                _ => new Point(0, 0)
            },
            2 => yDiff switch
            {
                > 0 => new Point(1, 1),
                0 => new Point(1, 0),
                < 0 => new Point(1, -1)
            },
            _ => throw new ApplicationException($"Unexpected xDiff: {xDiff}")
        };

        followingKnot.Offset(tailVelocity);

        this._knotPoses.Remove(knotNumToFollow);
        this._knotPoses.Add(knotNumToFollow, followingKnot);
    }

    public override void SolvePart1()
    {
        this._pointsVisitedByKnot1.Add(this._knotPoses[1]);
        
        this.Part1Solution = this._pointsVisitedByKnot1.Count.ToString();
    }

    public override void SolvePart2()
    {
        this._pointsVisitedByKnot9.Add(this._knotPoses[9]);

        this.Part2Solution = this._pointsVisitedByKnot9.Count.ToString();
    }
}