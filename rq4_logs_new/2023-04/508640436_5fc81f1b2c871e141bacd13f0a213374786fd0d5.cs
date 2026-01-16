using System.Text.Json.Serialization;

namespace car_racing_tournament_api.Models
{
    public class Statistics {
        public string? Name { get; set; }
        public int? NumberOfRace { get; set; }
        public int? NumberOfWin { get; set; }
        public int? NumberOfPodium { get; set; }
        public int? NumberOfChampion { get; set; }
        public int? SumPoint { get; set; }
        public List<SeasonStatistics>? SeasonStatistics { get; set; }
        public List<PositionStatistics>? PositionStatistics { get; set; }
    }

    public class SeasonStatistics {
        public Guid Id { get; set; }
        public string? Name { get; set; }
        public string? Color { get; set; }
        public DateTime CreatedAt { get; set; }
        public int? Position { get; set; }
    }

    public class PositionStatistics {
        public string? Position { get; set; }
        public int? Number { get; set; }
    }
}