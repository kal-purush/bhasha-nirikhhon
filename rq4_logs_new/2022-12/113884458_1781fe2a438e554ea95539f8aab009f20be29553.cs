namespace _2022.Days;

public class Day6 : Day
{
    private int _startOfPacketMarker = 0;
    private int _startOfMessageMarker = 0;
    
    public Day6() : base(6)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        var currentCandidate = new LinkedList<char>();
        var numCharactersRead = 0;
        var numToSkip = 0;

        foreach (var c in line)
        {
            numCharactersRead++;
            
            currentCandidate.AddLast(c);

            if (numToSkip > 0)
            {
                currentCandidate.RemoveFirst();
                numToSkip--;
                continue;
            }
            
            if (this._startOfPacketMarker is 0)
            {
                // Solving part 1
                if (currentCandidate.Count < 4)
                {
                    continue;
                }

                numToSkip = GetCharSkip(currentCandidate);

                if (numToSkip is -1)
                {
                    this._startOfPacketMarker = numCharactersRead;
                    numToSkip = 0;
                }
            }
            else
            {
                // Solving part 2
                if (currentCandidate.Count < 14)
                {
                    continue;
                }

                numToSkip = GetCharSkip(currentCandidate);

                if (numToSkip is -1)
                {
                    this._startOfMessageMarker = numCharactersRead;
                    break;
                }
            }
            
            currentCandidate.RemoveFirst();
        }
    }

    private static int GetCharSkip(LinkedList<char> data)
    {
        var seenCharacters = new HashSet<char>();

        var currentNode = data.Last;

        if (currentNode is null)
            throw new ArgumentException("Expected non-empty list");

        var numToSkip = data.Count - 1;

        do
        {
            var c = currentNode.Value;

            if (seenCharacters.Contains(c))
            {
                return numToSkip;
            }

            seenCharacters.Add(c);

            numToSkip--;
            currentNode = currentNode.Previous;
        } while (currentNode is not null);

        return numToSkip;
    }

    public override void SolvePart1()
    {
        this.Part1Solution = this._startOfPacketMarker.ToString();
    }

    public override void SolvePart2()
    {
        this.Part2Solution = this._startOfMessageMarker.ToString();
    }
}