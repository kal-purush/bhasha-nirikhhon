namespace PocketForzaHorizonCommunity.Back.DTO.DTOs.StatisticsDtos;

public class RecordsStatisticsDto
{
    public int HighestDriftScore { get; set; }
    public double HighestDangerSignScore { get; set; }
    public double HighestSpeedTrapScore { get; set; }
    public double HighestSpeedZoneScore { get; set; }
    public TimeSpan LongestSkillChain { get; set; }
    public double TopSpeed { get; set; }
    public double AvarageSpeed { get; set; }
    public int DistanceDriven { get; set; }
    public double LongestDrift { get; set; }
    public double LongestJump { get; set; }
}