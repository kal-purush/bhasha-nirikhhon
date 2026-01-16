using ReptileTracker.Feeding.Model;
using ReptileTracker.Shedding.Model;

namespace ReptileTracker.Animal.Model;

public class Reptile
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Species { get; set; }
    public DateTime Birthdate { get; set; }
    public ReptileType ReptileType { get; set; }
    public IList<Length> MeasurmentHistory { get; set; }
    public IList<Weight> WeightHistory { get; set; }
    public IList<FeedingEvent> FeedingHistory { get; set; }
    public IList<SheddingEvent> SheddingHistory { get; set; }
    
    public int AccountId { get; set; }
    public Account.Model.Account Account { get; set; }
}