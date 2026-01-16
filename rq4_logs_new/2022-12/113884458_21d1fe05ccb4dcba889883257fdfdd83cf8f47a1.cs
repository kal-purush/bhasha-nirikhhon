using System.Drawing;
using System.Globalization;

namespace _2022.Days;

public class Day15 : Day
{
    private readonly Dictionary<Point, int> _sensors = new();

    private readonly HashSet<Point> _interestingNoBeaconSpots = new();

    private const int YIndexOfInterestingLine = 2000000;
    private const int MaxDistressBeaconCoord = YIndexOfInterestingLine * 2;
    
    public Day15() : base(15)
    {
    }

    protected override void ProcessInputLine(string line)
    {
        var parts = line.Split(' ');

        if (parts.Length is not 10)
            throw new ArgumentException($"Got invalid line: {line}");
        
        // For each part remove the x= or y=, and the trailing , or :
        // Except for the last part, which ends at the end of the line
        var sensorX = int.Parse(string.Join("", parts[2].Substring(2, parts[2].Length - 3)));
        var sensorY = int.Parse(string.Join("", parts[3].Substring(2, parts[3].Length - 3)));
        
        var beaconX = int.Parse(string.Join("", parts[8].Substring(2, parts[8].Length - 3)));
        var beaconY = int.Parse(string.Join("", parts[9].Substring(2, parts[9].Length - 2)));

        var sensor = new Point(sensorX, sensorY);
        var beacon = new Point(beaconX, beaconY);
        
        this.ProcessSensorAndBeacon(sensor, beacon);
    }

    private void ProcessSensorAndBeacon(Point sensor, Point beacon)
    {
        var manDist = Math.Abs(sensor.X - beacon.X) + Math.Abs(sensor.Y - beacon.Y);
        var distToInterest = YIndexOfInterestingLine - sensor.Y;

        if (Math.Abs(distToInterest) <= manDist)
        {
            // Got overlap between the zone of denial and the interesting line
            var nearestIntersectPoint = new Point(sensor.X, sensor.Y + distToInterest);

            this._interestingNoBeaconSpots.Add(nearestIntersectPoint);

            var distanceSpreadAlongInterestLine = manDist - Math.Abs(distToInterest);

            for (var i = 1; i <= distanceSpreadAlongInterestLine; i++)
            {
                var left = nearestIntersectPoint with { X = nearestIntersectPoint.X - i };
                var right = nearestIntersectPoint with { X = nearestIntersectPoint.X + i };

                this._interestingNoBeaconSpots.Add(left);
                this._interestingNoBeaconSpots.Add(right);
            }
        }
        
        this._sensors.Add(sensor, manDist);
        
        // If there is a beacon on the line of interest then it's not a no-beacon spot!
        this._interestingNoBeaconSpots.Remove(beacon);
    }

    public override void SolvePart1()
    {
        this.Part1Solution = this._interestingNoBeaconSpots.Count.ToString();
    }

    public override void SolvePart2()
    {
        for (var y = 0; y <= MaxDistressBeaconCoord; y++)
        {
            var ranges = new List<Range>();

            foreach (var (sensor, manDist) in this._sensors)
            {
                var dy = Math.Abs(sensor.Y - y);

                if (dy > manDist)
                {
                    // No overlap
                    continue;
                }

                var rangeStart = sensor.X - (manDist - dy);
                var rangeEnd = sensor.X + (manDist - dy);
                
                ranges.Add(new Range(rangeStart, rangeEnd));
            }

            if (CheckIfGapInRow(ranges, out var gapX))
            {
                // Got a gap in this row!
                this.Part2Solution = (gapX * 4000000d + y).ToString(CultureInfo.InvariantCulture);
                return;
            }
        }
    }

    private static bool CheckIfGapInRow(IEnumerable<Range> ranges, out int gapX)
    {
        gapX = -1;
        
        // First collapse the ranges.
        var collapsedRanges = ranges
            .OrderBy(r => r.LowerBound)
            .Aggregate(new List<Range>(), (acc, range) =>
            {
                if (acc.Count is 0)
                {
                    acc.Add(range);
                }
                else
                {
                    var lastRange = acc.Last();

                    if (range.LowerBound <= lastRange.UpperBound)
                    {
                        // Overlap
                        lastRange.UpperBound = Math.Max(lastRange.UpperBound, range.UpperBound);
                    }
                    else if (range.LowerBound == lastRange.UpperBound + 1)
                    {
                        // Adjacent
                        lastRange.UpperBound = range.UpperBound;
                    }
                    else
                    {
                        // No overlap
                        acc.Add(range);
                    }
                }

                return acc;
            })
            .ToList();
        
        // Check if ranges cover entire relevant row
        foreach (var range in collapsedRanges.Where(range => range.LowerBound <= 0))
        {
            if (range.UpperBound >= MaxDistressBeaconCoord)
            {
                // Complete overlap
                return false;
            }
                
            // Only partial overlap!
            gapX = range.UpperBound + 1;
            return true;
        }

        return false;
    }

    private class Range
    {
        public readonly int LowerBound;
        public int UpperBound;

        public Range(int lowerBound, int upperBound)
        {
            this.LowerBound = lowerBound;
            this.UpperBound = upperBound;
        }

        public override string ToString()
        {
            return $"[{this.LowerBound},{this.UpperBound}]";
        }
    }
}