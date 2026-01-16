var input = File.ReadAllLines("input.txt")
        .Select(line => line.Split(' ').Where(x => x != "->"))
        .Select(points => points.Select(point => new Point(int.Parse(point.Split(',').First()), int.Parse(point.Split(',').Last()))))
        .Select(s => new Segment(s.First(), s.Last())).ToList();

Console.WriteLine($"Result A: {Result(input)}");
Console.WriteLine($"Result B: {Result(input, false)}");

static int Result(List<Segment> segments, bool skipDiagonal = true)
    => segments.Aggregate(new List<Point>(), (acc, s) =>
    {
        var dx = (s.A.X - s.B.X) switch 
        {
            < 0 => 1,
            > 0 => -1,
            _ => 0
        };

        var dy = (s.A.Y - s.B.Y) switch
        {
            < 0 => 1,
            > 0 => -1,
            _ => 0
        };

        if(dx != 0 && dy != 0 && skipDiagonal)
        {
            return acc;
        }

        var x = s.A.X;
        var y = s.A.Y;
        do
        {
            acc.Add(new Point(x, y));
            x += dx;
            y += dy;

        } while (acc.Last() != s.B);
        
        return acc;
    }, r => r.GroupBy(p => p).Select(g => (g.Key, g.Count())).Where(x => x.Item2 >= 2).Count());

record Point(int X, int Y);
record Segment(Point A, Point B);