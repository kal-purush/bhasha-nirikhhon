using System;
using System.Text;

namespace AdventOfCode.Days;

public class Day05 : BaseDay
{
    private readonly string[] _input;
    private int[][] _instructions;
    private int _stackWidth;
    private List<Stack<char>> _crates;
    private List<Stack<char>> _crates2;

    public Day05()
    {
        _input = File.ReadAllLines(InputFilePath);
        Setup();
    }

    public override ValueTask<string> Solve_1()
    {
        foreach (var instruction in _instructions)
        {
            var numberOfCratesToMove = instruction[0];
            var stackToMoveFrom = instruction[1] - 1;
            var stackToMoveTo = instruction[2] - 1;

            while (numberOfCratesToMove > 0)
            {
                var value = _crates[stackToMoveFrom].Pop();
                _crates[stackToMoveTo].Push(value);
                numberOfCratesToMove--;
            }
        }

        var result = new StringBuilder();
        foreach (var crateStack in _crates)
        {
            result.Append(crateStack.Pop());
        }

        return new(result.ToString());
    }

    public override ValueTask<string> Solve_2()
    {
        foreach (var instruction in _instructions)
        {
            var numberOfCratesToMove = instruction[0];
            var stackToMoveFrom = instruction[1] - 1;
            var stackToMoveTo = instruction[2] - 1;
            var cratesToMove = new char[numberOfCratesToMove];

            while (numberOfCratesToMove > 0)
            {
                var value = _crates2[stackToMoveFrom].Pop();
                cratesToMove[--numberOfCratesToMove] = value;
            }

            foreach (var crate in cratesToMove)
            {
                _crates2[stackToMoveTo].Push(crate);
            }
        }

        var result = new StringBuilder();
        foreach (var crateStack in _crates2)
        {
            result.Append(crateStack.Pop());
        }

        return new(result.ToString());
    }

    private List<Stack<char>> CreateCrates(List<string> stacks)
    {
        var crates = new List<Stack<char>>();
        for (int i = 0; i < _stackWidth; i++)
        {
            crates.Add(new Stack<char>());
        }

        foreach (var row in stacks)
        {
            var stringBuilder = new StringBuilder();
            for (int i = 1; i < row.Length; i += 4)
            {
                stringBuilder.Append(row[i]);
            }

            var rowCharacters = stringBuilder.ToString();
            for (int i = 0; i < rowCharacters.Length; i++)
            {
                if (!char.IsWhiteSpace(rowCharacters[i]))
                {
                    crates[i].Push(rowCharacters[i]);
                }
            }
        }

        return crates;
    }

    private void Setup()
    {
        var stacks = _input.TakeWhile(x => !string.IsNullOrEmpty(x)).ToList();
        _stackWidth = int.Parse(stacks[^1].Trim()[^1].ToString());

        stacks.Remove(stacks[^1]);
        stacks.Reverse();

        _crates = CreateCrates(stacks);
        _crates2 = CreateCrates(stacks);

        var instructions = _input[(stacks.Count + 2)..];

        _instructions = CreateInstructions(instructions);
    }

    private int[][] CreateInstructions(string[] instructions)
    {
        return instructions
            .Select(x => x.Split(" ")
                        .Where(y => int.TryParse(y, out var _))
                        .Select(int.Parse)
                        .ToArray())
            .ToArray();
    }
}
