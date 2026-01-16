var input = File.ReadAllLines("input.txt").Select(x =>
{
    var line = x.Split(' ');
    Enum.TryParse(line[0], out Operation operation);
    return new Instruction(operation, int.Parse(line[1]));
}).ToArray();


var horizontal = input.Where(x => x.Operation == Operation.forward)
    .Select(x => x.Value)
    .Sum();
var depth = input.Where(x => x.Operation != Operation.forward)
    .Select(x => x.Operation == Operation.down ? x.Value : -x.Value)
    .Sum();

Console.WriteLine($"Result A: {horizontal * depth}");

horizontal = 0;
depth = 0;
var aim = 0;
foreach (var i in input)
{
    switch (i.Operation)
    {
        case Operation.down:
            aim += i.Value;
            break;
        case Operation.up:
            aim -= i.Value;
            break;
        case Operation.forward:
            horizontal += i.Value;
            depth += aim * i.Value;
            break;
    }
}

Console.WriteLine($"Result B: {horizontal * depth}");

public record Instruction(Operation Operation, int Value);
public record Position(int Horizontal, int Depth, int Aim);
public enum Operation
{
    forward,
    down,
    up,
}
