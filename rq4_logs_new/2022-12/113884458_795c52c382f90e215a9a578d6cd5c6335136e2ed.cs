using System.Drawing;
using System.Text;

namespace _2022.Days;

public class Day12 : Day
{
    private Point _start;
    private Point _end;
    private Dictionary<Point, int> _heightMap = new();

    private int _y = 0;

    private int _maxX = 0;
    private int _maxY = 0;

    public Day12() : base(12)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        for (var x = 0; x < line.Length; x++)
        {
            var chr = line[x];
            var pos = new Point(x, this._y);
            int height;

            switch (line[x])
            {
                case 'S':
                    this._start = pos;
                    height = 0;
                    break;
                case 'E':
                    this._end = pos;
                    height = 25;
                    break;
                default:
                    height = chr - 'a';
                    break;
            }
            
            this._heightMap.Add(pos, height);
        }

        this._maxX = line.Length - 1;
        this._y++;
    }

    public override void SolvePart1()
    {
        this._maxY = this._y - 1;

        var considered = new HashSet<Point>();

        var queue = new Queue<(Point, int)>();
        
        queue.Enqueue((this._start, 0));

        while (queue.TryDequeue(out var data))
        {
            var (curPos, distance) = data;
            
            if (curPos == this._end)
            {
                this.Part1Solution = distance.ToString();
                return;
            }

            if (considered.Contains(curPos))
            {
                continue;
            }
            
            considered.Add(curPos);
            
            var curHeight = this._heightMap[curPos];
            var newDistance = distance + 1;

            var up = curPos with { Y = curPos.Y - 1 };
            var down = curPos with { Y = curPos.Y + 1 };
            var left = curPos with { X = curPos.X - 1 };
            var right = curPos with { X = curPos.X + 1 };

            // For each point, check it is within bounds, has not already been considered, 
            if (considered.Contains(up) is false && up.Y >= 0 && this._heightMap[up] - curHeight <= 1)
                queue.Enqueue((up, newDistance));
            
            if (considered.Contains(down) is false && down.Y <= this._maxY && this._heightMap[down] - curHeight <= 1)
                queue.Enqueue((down, newDistance));
            
            if (considered.Contains(left) is false && left.X >= 0 && this._heightMap[left] - curHeight <= 1)
                queue.Enqueue((left, newDistance));
            
            if (considered.Contains(right) is false && right.X <= this._maxX && this._heightMap[right] - curHeight <= 1)
                queue.Enqueue((right, newDistance));
        }
        
        Console.WriteLine($"Considered {considered.Count} points, but never got to the end!");

        this.Part1Solution = "ERROR";
    }

    public override void SolvePart2()
    {
        var considered = new HashSet<Point>();

        var queue = new Queue<(Point, int)>();

        foreach (var (pos, height) in this._heightMap)
        {
            if (height is 0)
                queue.Enqueue((pos, 0));
        }
        
        while (queue.TryDequeue(out var data))
        {
            var (curPos, distance) = data;
            
            if (curPos == this._end)
            {
                this.Part2Solution = distance.ToString();
                return;
            }

            if (considered.Contains(curPos))
            {
                continue;
            }
            
            considered.Add(curPos);
            
            var curHeight = this._heightMap[curPos];
            var newDistance = distance + 1;

            var up = curPos with { Y = curPos.Y - 1 };
            var down = curPos with { Y = curPos.Y + 1 };
            var left = curPos with { X = curPos.X - 1 };
            var right = curPos with { X = curPos.X + 1 };

            // For each point, check it is within bounds, has not already been considered, 
            if (considered.Contains(up) is false && up.Y >= 0 && this._heightMap[up] - curHeight <= 1)
                queue.Enqueue((up, newDistance));
            
            if (considered.Contains(down) is false && down.Y <= this._maxY && this._heightMap[down] - curHeight <= 1)
                queue.Enqueue((down, newDistance));
            
            if (considered.Contains(left) is false && left.X >= 0 && this._heightMap[left] - curHeight <= 1)
                queue.Enqueue((left, newDistance));
            
            if (considered.Contains(right) is false && right.X <= this._maxX && this._heightMap[right] - curHeight <= 1)
                queue.Enqueue((right, newDistance));
        }
        
        Console.WriteLine($"Considered {considered.Count} points, but never got to the end!");

        this.Part2Solution = "ERROR";
    }
}