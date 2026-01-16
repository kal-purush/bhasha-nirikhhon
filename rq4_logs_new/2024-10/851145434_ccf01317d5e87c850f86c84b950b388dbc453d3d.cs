using System.Text.RegularExpressions;

namespace JiraInteraction.Dtos;

public class EstimateDataDto
{
    public EstimateDataDto(
        string jiraidentifier,
        string? estimateTime,
        string? remainingTime)
    {
        Jiraidentifier = jiraidentifier;

        EstimateTime = (estimateTime == null && remainingTime == null) ?
            null : new EstimateTime
            {
                EstimateTimeInSeconds = GetNumberFromString(estimateTime),
                RemainingTimeInSeconds = GetNumberFromString(remainingTime)
            };
    }

    public string Jiraidentifier { get; set; }

    
    public EstimateTime? EstimateTime { get; set; }

    private int? GetNumberFromString(string? content)
    {
        if (string.IsNullOrEmpty(content))
        {
            return null;        
        }

        var numberPattern = @"[0-9]{1,}";
        var numberRegex = new Regex(numberPattern);

        var hourPattern = @"[0-9]{1,}h";
        var hRegex = new Regex(hourPattern);

        var minPattern = @"[0-9]{1,}m";
        var mRegex = new Regex(minPattern);

        var time = 0;
        var hStr = hRegex.Match(content);
        if (int.TryParse(hStr?.Value?.Replace("h", ""), out var hourTime))
        {
            time += hourTime * 60 * 60;
        }

        var mStr = mRegex.Match(content);
        if (int.TryParse(mStr?.Value?.Replace("m", ""), out var minuteTime))
        {
            time += minuteTime * 60;
        }
        return time;
    }
}

public class EstimateTime
{
    public long? EstimateTimeInSeconds { get; set; }

    public long? RemainingTimeInSeconds { get; set; }

}