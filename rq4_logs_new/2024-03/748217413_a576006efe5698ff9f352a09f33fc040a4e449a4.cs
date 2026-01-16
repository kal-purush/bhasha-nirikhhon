namespace Map.Domain.Models.Step;
public class StepDtoList
{
    public Guid StepId { get; set; }
    public Guid TripId { get; set; }
    public int StepNumber { get; set; }
    public string? Name { get; set; }

    public DateTime? StartDate { get; set; }
    public DateTime? EndDate { get; set; }

    //TODO update this when TravelTo and Reservation are implemented
    //public virtual IList<TravelTo>? TravelsTo { get; set; }
    //public virtual IList<Reservation>? Reservations { get; set; }
}