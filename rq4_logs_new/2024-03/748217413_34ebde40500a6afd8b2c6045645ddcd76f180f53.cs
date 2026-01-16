namespace Map.Domain.Entities;

public class Travel
{
    public Guid TravelId { get; set; }
    public Guid OriginStepId { get; set; }
    public virtual Step? OriginStep { get; set; }
    public Guid DestinationStepId { get; set; }
    public virtual Step? DestinationStep { get; set; }
    public string? TransportMode { get; set; }

    public decimal Distance { get; set; }
    public decimal Duration { get; set; }
    public int? CarbonEmition { get; set; }


    public virtual TravelRoad? TravelRoad { get; set; }
}