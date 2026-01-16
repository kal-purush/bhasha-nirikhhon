namespace AdventOfCode.Days;

public class Day06 : BaseDay
{
    private readonly string _input;

    public Day06()
    {
        _input = File.ReadAllText(InputFilePath);
    }
    public override ValueTask<string> Solve_1()
    {
        const int PacketSize = 4;

        return new(FindPacketSize(PacketSize).ToString());
    }

    public override ValueTask<string> Solve_2()
    {
        const int PacketSize = 14;

        return new(FindPacketSize(PacketSize).ToString());
    }

    private int FindPacketSize(int packetSize)
    {
        int iteration;
        for (iteration = 0; iteration < _input.Length; iteration++)
        {
            var packet = _input[iteration..(iteration + packetSize)];

            var distinctPacketCharacters = packet.Distinct();

            if (distinctPacketCharacters.Count() == packetSize)
            {
                break;
            }
        }

        return iteration + packetSize;
    }
}