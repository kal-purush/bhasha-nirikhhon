using System.Globalization;

namespace _2022.Days;

public class Day20 : Day
{
    private const double DecryptionKey = 811589153;
    private readonly List<double> _numbers = new(); 
    
    public Day20() : base(20)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        if (double.TryParse(line, out var number) is false)
            throw new ArgumentException($"Expected {line} to be a number", nameof(line));

        this._numbers.Add(number);
    }

    protected override void SolvePart1()
    {
        var orderedNumbers = this._numbers.Select((n, i) => (number: n, originalIndex: i)).ToList();

        Mix(orderedNumbers);
        
        // Console.WriteLine(string.Join(',', orderedNumbers));

        var (x, y, z) = GetGroveCoordinates(orderedNumbers);

        this.Part1Solution = (x + y + z).ToString(CultureInfo.InvariantCulture);
    }

    protected override void SolvePart2()
    {
        var orderedNumbers = this._numbers
            .Select((n, i) => (number: n * DecryptionKey, originalIndex: i)).ToList();

        for (var i = 1; i <= 10; i++)
        {
            Mix(orderedNumbers);
        }

        var (x, y, z) = GetGroveCoordinates(orderedNumbers);

        this.Part2Solution = (x + y + z).ToString(CultureInfo.InvariantCulture);
    }

    private static void Mix(List<(double number, int originalIndex)> data)
    {
        for (var i = 0; i < data.Count; i++)
        {
            // Console.WriteLine(string.Join(',', data));
            
            var orderedIndex = data.FindIndex((entry) => entry.originalIndex == i);

            var number = data[orderedIndex];
            
            data.RemoveAt(orderedIndex);
            
            // How far are we moving?
            var newIndex = (orderedIndex + number.number) % data.Count;

            if (newIndex < 0)
            {
                newIndex = data.Count + newIndex;
            }
            
            data.Insert((int) newIndex, number);
        }
    }

    private static (double x, double y, double z) GetGroveCoordinates(List<(double number, int originalIndex)> data)
    {
        var startIndex = data.FindIndex((entry) => entry.number is 0);

        var firstCoordIndex = (startIndex + 1000) % data.Count;
        var secondCoordIndex = (startIndex + 2000) % data.Count;
        var thirdCoordIndex = (startIndex + 3000) % data.Count;

        var firstCoord = data[firstCoordIndex];
        var secondCoord = data[secondCoordIndex];
        var thirdCoord = data[thirdCoordIndex];

        return (firstCoord.number, secondCoord.number, thirdCoord.number);
    }
}