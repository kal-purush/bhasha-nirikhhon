using System.ComponentModel.DataAnnotations;

namespace APIClient
{
    public class LocalImpact
    {
        public DateTime Date { get; set; }
        [Required]
        public string? Location { get; set; }
        public string? DamageScale { get; set; }
        public int? LocationDamage { get; set; }

    }
}