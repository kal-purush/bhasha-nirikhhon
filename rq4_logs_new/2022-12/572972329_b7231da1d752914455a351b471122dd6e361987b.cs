using AdventOfCodeUtilities.Helpers;

namespace AdventOfCode.Days;

public class Day10 : BaseDay
{
    private readonly List<(string Instruction, int Value)> _input;
    private readonly int _cycles;

    public Day10()
    {
        _input = File.ReadAllLines(InputFilePath)
            .Select(x => x.Split(" "))
            .Select(x => (x[0], x.Length == 2 ? int.Parse(x[1]) : 1))
            .ToList();

        _cycles = _input.Select(x => x.Instruction == "noop" ? 1 : 2).Sum();
    }

    public override ValueTask<string> Solve_1()
    {
        var register = 1;
        var signalStrength = 0;
        var totalCycles = _cycles;
        var cycle = 0;
        var cycleToCheckAt = 20;
        var instructionIndex = 0;

        var addXExecuted = false;
        while (cycle < totalCycles)
        {
            cycle++;

            var instruction = _input[instructionIndex];

            if (cycle == cycleToCheckAt)
            {
                signalStrength += register * cycle;
                cycleToCheckAt += 40;
            }

            if (instruction.Instruction != "noop")
            {
                if (addXExecuted)
                {
                    addXExecuted = false;
                    register += instruction.Value;

                }
                else
                {
                    addXExecuted = true;
                    continue;
                }
            }

            instructionIndex++;
        }

        return new(signalStrength.ToString());
    }

    public override ValueTask<string> Solve_2()
    {
        var register = 1;
        var totalCycles = _cycles;
        var cycle = 0;
        var cycleToCheckAt = 40;
        var instructionIndex = 0;
        var crtRow = 0;
        var pixelPosition = 0;
        var crt = new char[6][]
        {
            new char[40],
            new char[40],
            new char[40],
            new char[40],
            new char[40],
            new char[40]
        };

        var addXExecuted = false;
        while (cycle < totalCycles)
        {
            cycle++;

            var instruction = _input[instructionIndex];

            if (cycle == cycleToCheckAt + 1)
            {
                cycleToCheckAt += 40;
                crtRow++;
            }

            crt = UpdateCrt(crt, register - 1, pixelPosition, crtRow);

            if (instruction.Instruction != "noop")
            {
                if (addXExecuted)
                {
                    addXExecuted = false;
                    register += instruction.Value;

                }
                else
                {
                    addXExecuted = true;
                    instructionIndex--;
                }
            }

            pixelPosition++;
            instructionIndex++;
        }

        ArrayHelper.ArrayPrinter(crt);
        return new(1.ToString());
    }

    private char[][] UpdateCrt(char[][] crt, int spritePosition, int pixelPosition, int crtRow)
    {
        pixelPosition -= (40 * crtRow);

        crt[crtRow][pixelPosition] = spritePosition == pixelPosition || spritePosition + 1 == pixelPosition || spritePosition + 2 == pixelPosition ? '#' : '.';

        return crt;
    }
}