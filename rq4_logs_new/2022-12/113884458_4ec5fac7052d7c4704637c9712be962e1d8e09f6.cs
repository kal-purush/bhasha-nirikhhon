using Figgle;

namespace _2022.Days;

public abstract class Day
{
    private readonly int _dayNum;
    protected string Part1Solution = "";
    protected string Part2Solution = "";

    protected Day(int dayNum)
    {
        _dayNum = dayNum;
    }

    public void ProcessInput()
    {
        var fileName = $"Inputs/day{this._dayNum}.txt";
        var filePath = Path.Combine(Environment.CurrentDirectory, fileName);

        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException("Failed to find input file", filePath);
        }

        using var stream = new StreamReader(filePath);
        
        while (!stream.EndOfStream)
        {
            var line = stream.ReadLine();

            if (line != null)
                this.ProcessInputLine(line);
        }
    }

    protected abstract void ProcessInputLine(string line);

    public void PrintSolution()
    {
        Console.WriteLine(FiggleFonts.Banner3.Render($"Advent of Code Day {this._dayNum}"));
        Console.WriteLine();
        Console.WriteLine();
        Console.WriteLine("Part 1:");
        Console.WriteLine(this.Part1Solution);
        Console.WriteLine();
        Console.WriteLine("Part 2:");
        Console.WriteLine(this.Part2Solution);
    }
    
    public abstract void SolvePart1();

    public abstract void SolvePart2();
}