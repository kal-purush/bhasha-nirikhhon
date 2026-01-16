using System;

namespace AwakenServer.Activity;

public class RandomSnapshotHelper
{
    public static TimeSpan GetNextLpSnapshotExecutionTime(Random random, DateTime lastExecuteTime)
    {
        var randomMinute = random.Next(50, 70) % 60;
        var randomSecond = random.Next(0, 60);
        var lastSnapshotHour = lastExecuteTime.Hour;
        if (lastExecuteTime.Minute >= 50)
        {
            lastSnapshotHour = (lastSnapshotHour + 1) % 24;
        }

        if (randomMinute >= 50)
        {
            return new TimeSpan(lastSnapshotHour, randomMinute, randomSecond);
        }
        else
        {
            return new TimeSpan((lastSnapshotHour + 1) % 24, randomMinute, randomSecond);
        }
    }
    
    public static DateTime GetLpSnapshotTime(DateTime timestamp)
    {
        if (timestamp.Minute <= 10)
        {
            return new DateTime(timestamp.Year, timestamp.Month, timestamp.Day, timestamp.Hour, 0, 0);
        }

        if (timestamp.Minute >= 50)
        {
            DateTime nextHour = timestamp.AddHours(1);
            return new DateTime(nextHour.Year, nextHour.Month, nextHour.Day, nextHour.Hour, 0, 0);
        }

        return new DateTime(timestamp.Year, timestamp.Month, timestamp.Day, timestamp.Hour, 0, 0);
        ;
    }
}