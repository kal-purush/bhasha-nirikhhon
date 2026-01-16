using System.Threading.Tasks.Sources;

namespace AdventOfCode.Day08;

public class Day08 : Day
{
    public Day08()
    {
        Map = ParseMap(_input)
    }

    private double[][] Map { get; set; }

    public override ValueTask<string> Solve_1()
    {
        return new(GetPoints(Map).Count().ToString());
    }

    private IEnumerable<(int X, int Y)> GetPoints(double[][] map)
    {
        for (int x = 0; x < map.Length; x++)
        {
            var line = map[x];
            for (int y = 0; y < line.Length; y++)
            {
                if (IsVisible(map, x, y))
                {
                    yield return (x, y);
                }
            }
        }
    }

    private bool IsVisible(double[][] map, int x, int y)
    {
        if (x == 0 || y == 0 || x == (map.Length - 1) || y == (map[0].Length - 1))
        {
            return true;
        }

        // Left
        return Enumerable.Range(0, x).All(curX => map[curX][y] < map[x][y]) ||
               // Top
               Enumerable.Range(0, y).All(curY => map[x][curY] < map[x][y]) ||
               // Right
               Enumerable.Range(x + 1, map[0].Length - x - 1).All(curX => map[curX][y] < map[x][y]) ||
               // Bottom
               Enumerable.Range(y + 1, map.Length - y - 1).All(currY => map[x][currY] < map[x][y]);
    }

    private double[][] ParseMap(string input)
        => input.SplitNewLine().Select(x => x.ToCharArray().Select(char.GetNumericValue).ToArray()).ToArray();

    public override ValueTask<string> Solve_2()
        => throw new NotImplementedException();
}