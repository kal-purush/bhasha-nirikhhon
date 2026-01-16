using System.Drawing;

namespace _2022.Days;

public class Day14 : Day
{
    private readonly HashSet<Point> _rock = new();
    private readonly HashSet<Point> _restingSand = new();

    private readonly Point _startPoint = new(500, 0);
    private int _maxY = 0;
    
    public Day14() : base(14)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        var coords = line.Split(" -> ");

        Point? lastCoord = null;

        foreach (var cornerStr in coords)
        {
            var parts = cornerStr.Split(',').Select(int.Parse).ToList();

            var corner = new Point(parts[0], parts[1]);

            this._maxY = Math.Max(this._maxY, corner.Y);

            if (lastCoord.HasValue is false)
            {
                lastCoord = corner;
                continue;
            }

            var velocity = new Point(Math.Sign(corner.X - lastCoord.Value.X), Math.Sign(corner.Y - lastCoord.Value.Y));

            while (lastCoord != corner)
            {
                this._rock.Add(lastCoord.Value);

                lastCoord = new Point(lastCoord.Value.X + velocity.X, lastCoord.Value.Y + velocity.Y);
            }

            this._rock.Add(lastCoord.Value);
        }
    }

    public override void SolvePart1()
    {
        var sandGrain = new Point(this._startPoint.X, this._startPoint.Y);

        // Keep looping until we get a grain that falls below the lowest known rock
        while (sandGrain.Y <= this._maxY)
        {
            if (this.CanMove(sandGrain, out var newSandGrain))
            {
                sandGrain = newSandGrain;
            }
            else
            {
                // Cannot move to any of the 3 candidate spots; sand grain rests here
                this._restingSand.Add(sandGrain);

                sandGrain = new Point(this._startPoint.X, this._startPoint.Y);    
            }
        }

        this.Part1Solution = this._restingSand.Count.ToString();
    }

    private bool CanMove(Point sandGrain, out Point newPos)
    {
        newPos = sandGrain with { Y = sandGrain.Y + 1 };

        if (this.IsClear(newPos))
        {
            // Can fall down, do it.
            return true;
        }
            
        // Try down-left
        newPos.Offset(new Point(-1, 0));

        if (this.IsClear(newPos))
        {
            // Can fall down-left, do it.
            return true;
        }
            
        // Try down-right (we have already moved left 1)
        newPos.Offset(new Point(2, 0));

        return this.IsClear(newPos);
    }
    
    private bool IsClear(Point p)
    {
        return p.Y < this._maxY + 2 && this._rock.Contains(p) is false && this._restingSand.Contains(p) is false;
    }

    public override void SolvePart2()
    {
        // Having already run part 1, keep adding sand until the start is blocked.
        var sandGrain = new Point(this._startPoint.X, this._startPoint.Y);
        
        while (this._restingSand.Contains(this._startPoint) is false)
        {
            if (this.CanMove(sandGrain, out var newSandGrain))
            {
                sandGrain = newSandGrain;
            }
            else
            {
                this._restingSand.Add(sandGrain);
                sandGrain = new Point(this._startPoint.X, this._startPoint.Y);
            }
        }

        this.Part2Solution = this._restingSand.Count.ToString();
    }
}