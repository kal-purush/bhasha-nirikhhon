using System.Text.Json.Nodes;

namespace _2022.Days;

public class Day13 : Day
{
    private readonly HashSet<int> _orderedIndexes = new();

    private int _index = 1;
    private JsonNode? _firstPacket = null;

    private List<JsonNode> _packets = new();

    public Day13() : base(13)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        if (line.Length is 0)
        {
            // Empty line between packets
            this._firstPacket = null;
            this._index++;
            return;
        }
        
        this._packets.Add(JsonNode.Parse(line));
        
        if (this._firstPacket is null)
        {
            // First packet line
            this._firstPacket = JsonNode.Parse(line);
            return;
        }

        // Second packet
        var comp = ComparePackets(this._firstPacket, JsonNode.Parse(line));

        if (comp < 0)
        {
            this._orderedIndexes.Add(this._index);
        }
    }

    private static int ComparePackets(JsonNode? first, JsonNode? second)
    {
        if (first is null || second is null)
            throw new ArgumentException("Got null JsonNodes");
        
        if (first is JsonValue && second is JsonValue)
            return (int)first - (int)second;

        var firstArray = first as JsonArray ?? new JsonArray((int)first);
        var secondArray = second as JsonArray ?? new JsonArray((int)second);

        return firstArray.Zip(secondArray)
            .Select(p => ComparePackets(p.First, p.Second))
            .FirstOrDefault(c => c != 0, firstArray.Count - secondArray.Count);
    }

    protected override void SolvePart1()
    {
        this.Part1Solution = this._orderedIndexes.Sum().ToString();
    }

    protected override void SolvePart2()
    {
        var orderedPackets = this._packets.OrderBy(n => n, new PacketComparer()).ToList();

        var firstPacket = JsonNode.Parse("[[2]]");
        var secondPacket = JsonNode.Parse("[[6]]");

        var firstIndex = 0;

        while (ComparePackets(firstPacket, orderedPackets[firstIndex]) > 0)
        {
            firstIndex++;
        }

        var secondIndex = firstIndex;

        while (ComparePackets(secondPacket, orderedPackets[secondIndex]) > 0)
        {
            secondIndex++;
        }
        
        // Never added first packet!
        this.Part2Solution = ((firstIndex + 1) * (secondIndex + 2)).ToString();
    }

    private class PacketComparer : IComparer<JsonNode>
    {
        public int Compare(JsonNode x, JsonNode y)
        {
            return ComparePackets(x, y);
        }
    }
}