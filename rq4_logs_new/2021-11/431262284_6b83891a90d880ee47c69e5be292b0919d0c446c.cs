namespace TftTracker.Data.Entities
{
    public class MatchInfo
    {
        public int Id { get; set; }
        public string MatchId { get; set; }
        public Match Match { get; set; }
        public long GameDateTime { get; set; }
        public float GameLength { get; set; }
        public string GameVariation { get; set; }
        public string GameVersion { get; set; }
        public ICollection<Participant> Participants { get; set; }
        public int QueueId { get; set; }
        public int TftSetNumber { get; set; }
    }
}