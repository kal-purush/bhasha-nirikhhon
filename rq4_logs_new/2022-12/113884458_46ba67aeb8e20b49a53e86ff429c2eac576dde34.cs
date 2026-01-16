using System.Collections.Concurrent;
using System.Drawing;

namespace _2022.Days;

public class Day23 : Day
{
    private int _y;

    private readonly HashSet<Point> _elves = new();

    private readonly List<Direction> _directionsToConsider = new()
    {
        Direction.North,
        Direction.South,
        Direction.West,
        Direction.East
    };
    
    public Day23() : base(23)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        for (var x = 0; x < line.Length; x++)
        {
            var chr = line[x];

            if (chr is '#')
            {
                var elf = new Point(x, this._y);
                this._elves.Add(elf);
            }
        }

        this._y++;
    }

    protected override void SolvePart1()
    {
        for (var i = 0; i < 10; i++)
        {
            this.MoveElves();
        }

        var topLeft = new Point(int.MaxValue, int.MaxValue);
        var bottomRight = new Point(int.MinValue, int.MinValue);

        foreach (var elf in this._elves)
        {
            topLeft = new Point(Math.Min(topLeft.X, elf.X), Math.Min(topLeft.Y, elf.Y));
            bottomRight = new Point(Math.Max(bottomRight.X, elf.X), Math.Max(bottomRight.Y, elf.Y));
        }

        var rectangleSize = (bottomRight.X - topLeft.X + 1) * (bottomRight.Y - topLeft.Y + 1);

        var numEmptySquares = rectangleSize - this._elves.Count;

        this.Part1Solution = numEmptySquares.ToString();
    }

    private bool MoveElves()
    {
        // Get candidate positions
        
        // Map of Point being moved to -> Elves moving to that point
        var candidates = new ConcurrentDictionary<Point, HashSet<Point>>();

        foreach (var elf in this._elves)
        {
            if (this.GetCandidatePoint(elf, out var candidatePoint))
            {
                candidates.AddOrUpdate(candidatePoint, _ => new HashSet<Point> { elf }, (_, elves) =>
                {
                    elves.Add(elf);
                    return elves;
                });    
            }
        }

        var hasMoved = false;
        
        // Candidates calculated - do the moves that don't conflict.
        foreach (var (newPos, elves) in candidates)
        {
            if (elves.Count is 1)
            {
                this._elves.Remove(elves.First());
                this._elves.Add(newPos);
                hasMoved = true;
            }
        }

        var firstDir = this._directionsToConsider.First();

        this._directionsToConsider.RemoveAt(0);
        this._directionsToConsider.Add(firstDir);

        return hasMoved;
    }

    private bool GetCandidatePoint(Point elf, out Point candidatePoint)
    {
        var hasAdjacentElf = false;

        for (var dx = -1; dx <= 1; dx++)
        {
            for (var dy = -1; dy <= 1; dy++)
            {
                if (dx is 0 && dy is 0)
                    continue;

                if (this._elves.Contains(new Point(elf.X + dx, elf.Y + dy)))
                    hasAdjacentElf = true;
            }
        }

        candidatePoint = new Point(0, 0);

        if (hasAdjacentElf is false)
            return false;

        foreach (var dir in this._directionsToConsider)
        {
            switch (dir)
            {
                case Direction.North:
                    if (this.IsClearNorth(elf))
                    {
                        candidatePoint = elf with { Y = elf.Y - 1 };
                        return true;
                    }
                    break;
                case Direction.South:
                    if (this.IsClearSouth(elf))
                    {
                        candidatePoint = elf with { Y = elf.Y + 1 };
                        return true;
                    }
                    break;
                case Direction.East:
                    if (this.IsClearEast(elf))
                    {
                        candidatePoint = elf with { X = elf.X + 1 };
                        return true;
                    }
                    break;
                case Direction.West:
                    if (this.IsClearWest(elf))
                    {
                        candidatePoint = elf with { X = elf.X - 1 };
                        return true;
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException($"Unexpected direction {dir}");
            }
        }

        // Completely surrounded.
        return false;
    }

    private bool IsClearNorth(Point elf)
    {
        for (var dx = -1; dx <= 1; dx++)
        {
            if (this._elves.Contains(new Point(elf.X + dx, elf.Y - 1)))
                return false;
        }

        return true;
    }

    private bool IsClearSouth(Point elf)
    {
        for (var dx = -1; dx <= 1; dx++)
        {
            if (this._elves.Contains(new Point(elf.X + dx, elf.Y + 1)))
                return false;
        }

        return true;
    }

    private bool IsClearEast(Point elf)
    {
        for (var dy = -1; dy <= 1; dy++)
        {
            if (this._elves.Contains(new Point(elf.X + 1, elf.Y + dy)))
                return false;
        }

        return true;
    }

    private bool IsClearWest(Point elf)
    {
        for (var dy = -1; dy <= 1; dy++)
        {
            if (this._elves.Contains(new Point(elf.X - 1, elf.Y + dy)))
                return false;
        }

        return true;
    }

    protected override void SolvePart2()
    {
        var numRounds = 10;

        do
        {
            numRounds++;
        } while (this.MoveElves());

        this.Part2Solution = numRounds.ToString();
    }

    private enum Direction
    {
        North,
        South,
        West,
        East
    }
}