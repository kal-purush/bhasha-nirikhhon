using System.Text;
using System.Text.RegularExpressions;

namespace _2022.Days;

public class Day5 : Day
{
    private readonly Regex _instructionRegex = new("^move ([0-9]+) from ([0-9]+) to ([0-9]+)$");
    private bool _readingInstructions = false;

    private List<Stack<char>>? _stacks;
    private List<Stack<char>>? _v2Stacks;

    public Day5() : base(5)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        if (this._readingInstructions)
        {
            if (this._stacks is null || this._v2Stacks is null)
                throw new ApplicationException("Stacks have not been initialised when reading instructions!");
            
            var match = this._instructionRegex.Match(line);

            var numToMove = int.Parse(match.Groups[1].Value);
            var fromStackId = int.Parse(match.Groups[2].Value) - 1;
            var toStackId = int.Parse(match.Groups[3].Value) - 1;

            var intermediateV2Stack = new Stack<char>();

            while (numToMove > 0)
            {
                this._stacks[toStackId].Push(this._stacks[fromStackId].Pop());
                intermediateV2Stack.Push(this._v2Stacks[fromStackId].Pop());
                numToMove--;
            }

            while (intermediateV2Stack.TryPop(out var item))
            {
                this._v2Stacks[toStackId].Push(item);
            }
        }
        else if (line.Length == 0)
        {
            // Empty line between initial stack and instructions
            this._readingInstructions = true;
        }
        else if (line[1] == '1')
        {
            // Stack numbers
            if (this._stacks is null)
                throw new ApplicationException("Stacks is null when trying to reverse them");

            var oldStacks = this._stacks;

            this._stacks = new List<Stack<char>>(oldStacks.Count);
            this._v2Stacks = new List<Stack<char>>(oldStacks.Count);

            foreach (var oldStack in oldStacks)
            {
                var newStack = new Stack<char>();
                var newV2Stack = new Stack<char>();

                while (oldStack.TryPop(out var item))
                {
                    newStack.Push(item);
                    newV2Stack.Push(item);
                }
                
                this._stacks.Add(newStack);
                this._v2Stacks.Add(newV2Stack);
            }
        }
        else
        {
            // Stack contents
            var numStacks = (line.Length + 1) / 4;

            if (this._stacks is null)
            {
                this._stacks = new List<Stack<char>>(numStacks);

                for (var i = 0; i < numStacks; i++)
                {
                    this._stacks.Add(new Stack<char>());
                }
            }

            for (var i = 0; i < numStacks; i++)
            {
                var item = line[(i * 4) + 1];

                if (item != ' ')
                {
                    this._stacks[i].Push(item);
                }
            }
        }
    }

    public override void SolvePart1()
    {
        var messageBuilder = new StringBuilder();

        if (this._stacks is null)
            throw new ApplicationException("Stacks is null when trying to get solution!");

        foreach (var stack in this._stacks)
        {
            messageBuilder.Append(stack.Peek());
        }

        this.Part1Solution = messageBuilder.ToString();
    }

    public override void SolvePart2()
    {
        var messageBuilder = new StringBuilder();

        if (this._v2Stacks is null)
            throw new ApplicationException("Stacks is null when trying to get solution!");

        foreach (var stack in this._v2Stacks)
        {
            messageBuilder.Append(stack.Peek());
        }

        this.Part2Solution = messageBuilder.ToString();
    }
}