namespace SooshEgoServer.Data
{
    public class CompletedGame
    {
        public int Id { get; set; }
        public DateTime TimeCompleted { get; set; } = DateTime.Now;

        public ICollection<PlayerPoints> PlayerPoints { get; set; } = [];
    }
}