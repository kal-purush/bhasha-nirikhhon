namespace MainService.Domain.Entities;
public class SprintDomain
    {
        public string? Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTime DateStarted { get; set; }
        public DateTime DateEnded { get; set; }
        public int Duration { get; set; }
        public string Goal { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
        public string ProjectId { get; set; } = string.Empty;
    }