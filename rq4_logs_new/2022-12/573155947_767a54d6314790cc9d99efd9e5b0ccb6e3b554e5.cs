using System.Collections;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks.Sources;

namespace AdventOfCode;

public class Day05 : BaseDay
{
    private readonly string _input;

    public Day05()
    {
        _input = File.ReadAllText(InputFilePath);
    }

    public override ValueTask<string> Solve_1()
    {
        var board = ParseInput(_input);
        PlayBoardPart1(board);
        return new(GetCode(board));
    }

    private string GetCode(Board board)
    {
        var sb = new StringBuilder();
        foreach (var stack in board.Stacks)
        {
            sb.Append(stack.Queue.Peek().Char);
        }

        return sb.ToString();
    }

    private void PlayBoardPart1(Board board)
    {
        while (board.Instructions.TryDequeue(out var current))
        {
            for (var i = 0; i < current.Count; i++)
            {
                board.Stacks[current.To - 1].Queue.Push(board.Stacks[current.From - 1].Queue.Pop());
            }
        }
    }

    private void PlayBoardPart2(Board board)
    {
        while (board.Instructions.TryDequeue(out var current))
        {
            var qeue = new Stack<Cargo>();
            for (var i = 0; i < current.Count; i++)
            {
                qeue.Push(board.Stacks[current.From - 1].Queue.Pop());
            }

            while (qeue.TryPop(out var currentitem))
            {
                board.Stacks[current.To - 1].Queue.Push(currentitem);
            }
        }
    }

    private Board ParseInput(string input)
    {
        var lines = input.SplitNewLine();
        var indexOfLines = lines.ToList().IndexOf("");
        var stackAmount = lines[indexOfLines - 1].SplitSpaceStripped().Last().AsInt();

        // Parse stacks
        var stacks = Enumerable.Range(0, stackAmount).Select(x => new Stack(new Stack<Cargo>())).ToArray();
        for (int i = 0; i < stacks.Length; i += 1)
        {
            var currentStack = stacks[i];
            for (int j = indexOfLines - 2; j >= 0; j--)
            {
                var toEnqueue = lines[j][1 + (i * 4)];
                if (toEnqueue != ' ')
                {
                    currentStack.Queue.Push(new Cargo(toEnqueue));
                }
            }
        }

        // Parse Instructions
        var instructions = lines[(indexOfLines + 1)..].Select(Instruction.Parse).ToArray();

        return new Board(stacks, new Queue<Instruction>(instructions));
    }

    public override ValueTask<string> Solve_2()
    {
        var board = ParseInput(_input);
        PlayBoardPart2(board);
        return new(GetCode(board));
    }
}

public record Board(Stack[] Stacks, Queue<Instruction> Instructions);

public record Stack(Stack<Cargo> Queue);

public record Cargo(char Char);

public record Instruction(long Count, long From, long To)
{
    public static Instruction Parse(string input)
    {
        var match = Regex.Match(input, @"move ([\d]+) from ([\d]+) to ([\d]+)");
        return new Instruction(match.Groups[1].Value.AsLong(), match.Groups[2].Value.AsLong(), match.Groups[3].Value.AsLong());
    }
};