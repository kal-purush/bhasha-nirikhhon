using System.Drawing;
using _2022.Utils;

namespace _2022.Days;

public class Day24 : Day
{
    private HashSet<(Point pos, Direction dir)> _blizzards = new();

    private int _y;

    private int _maxX;
    private int _maxY;

    private readonly Point _start = new Point(1, 0);
    private Point _end;
    
    private int _initialTimeFromStartToEnd;

    public Day24() : base(24)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        if (this._y is 0)
        {
            this._y++;
            return;
        }

        this._maxX = line.Length - 2;
        
        for (var x = 1; x <= this._maxX; x++)
        {
            var chr = line[x];

            (Point pos, Direction dir)? blizzard = chr switch
            {
                '<' => (pos: new Point(x, this._y), dir: Direction.West),
                '>' => (pos: new Point(x, this._y), dir: Direction.East),
                '^' => (pos: new Point(x, this._y), dir: Direction.North),
                'v' => (pos: new Point(x, this._y), dir: Direction.South),
                _ => null
            };

            if (blizzard.HasValue)
            {
                this._blizzards.Add(blizzard.Value);
            }
        }

        this._y++;
    }

    protected override void SolvePart1()
    {
        // We always increment y after reading the line, so after reading the last
        // (out of bounds) line it will be 2 over the maximum allowed y value.
        this._maxY = this._y - 2;
        
        this._end = new Point(this._maxX, this._maxY + 1);

        this._initialTimeFromStartToEnd = this.DoMinWalkBetween(this._start, this._end);
        
        this.Part1Solution = this._initialTimeFromStartToEnd.ToString();
    }

    private int DoMinWalkBetween(Point from, Point to)
    {
        var minute = 0;
        
        var currentPositions = new HashSet<Point> { from };

        while (true)
        {
            // Move blizzards
            var blizzardPositions = this.MoveBlizzardsAndReturnCoveredPositions();
            
            // Figure out what moves (up, down, left, right, wait) are valid, for each of the current positions
            // If one of those is the end then we're done.
            // Otherwise add them all to the next current positions
            var nextPositions = new HashSet<Point>();

            foreach (var curPos in currentPositions)
            {
                // Can wait in place.
                if (blizzardPositions.Contains(curPos) is false)
                    nextPositions.Add(curPos);

                var north = curPos with { Y = curPos.Y - 1 };
                var south = curPos with { Y = curPos.Y + 1 };
                var east = curPos with { X = curPos.X + 1 };
                var west = curPos with { X = curPos.X - 1 };

                if (blizzardPositions.Contains(south) is false)
                {
                    if (south == to)
                    {
                        return minute + 1;
                    }
                    
                    if (this.InBounds(south))
                    {
                        nextPositions.Add(south);
                    }
                }

                if (blizzardPositions.Contains(north) is false)
                {
                    if (north == to)
                    {
                        return minute + 1;
                    }

                    if (this.InBounds(north))
                    {
                        nextPositions.Add(north);
                    }
                }

                if (this.InBounds(east) && blizzardPositions.Contains(east) is false)
                {
                    nextPositions.Add(east);
                }

                if (this.InBounds(west) && blizzardPositions.Contains(west) is false)
                {
                    nextPositions.Add(west);
                }
            }

            currentPositions = nextPositions;
            
            minute++;
        }
    }

    private HashSet<Point> MoveBlizzardsAndReturnCoveredPositions()
    {
        var newBlizzards = new HashSet<(Point pos, Direction dir)>();
        var currentBlizzardPositions = new HashSet<Point>();

        foreach (var (pos, dir) in this._blizzards)
        {
            var newPos = dir switch
            {
                Direction.North => this.WrapPoint(pos with { Y = pos.Y - 1 }),
                Direction.South => this.WrapPoint(pos with { Y = pos.Y + 1 }),
                Direction.East => this.WrapPoint(pos with { X = pos.X + 1 }),
                Direction.West => this.WrapPoint(pos with { X = pos.X - 1 }),
                _ => throw new ArgumentOutOfRangeException(nameof(dir), $"Unexpected direction {dir}")
            };

            newBlizzards.Add((pos: newPos, dir));
            currentBlizzardPositions.Add(newPos);
        }

        this._blizzards = newBlizzards;

        return currentBlizzardPositions;
    }

    private Point WrapPoint(Point p)
    {
        if (p.X < 1)
            return p with { X = this._maxX };
        if (p.X > this._maxX)
            return p with { X = 1 };
        if (p.Y < 1)
            return p with { Y = this._maxY };
        if (p.Y > this._maxY)
            return p with { Y = 1 };
        return p;
    }

    private bool InBounds(Point p)
    {
        return p.X >= 1 && p.X <= this._maxX && p.Y >= 1 && p.Y <= this._maxY;
    }

    protected override void SolvePart2()
    {
        var t2 = this.DoMinWalkBetween(this._end, this._start);

        var t3 = this.DoMinWalkBetween(this._start, this._end);

        this.Part2Solution = (this._initialTimeFromStartToEnd + t2 + t3).ToString();
    }
}