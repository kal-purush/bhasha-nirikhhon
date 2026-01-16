namespace PocketForzaHorizonComunity.Back.DTO.Requests.Guides;

public class CreateTuneRequest
{
    public string Title { get; set; } = null!;
    public string ForzaShareCode { get; set; } = null!;
    public string AuthorId { get; set; } = null!;
    public string CarId { get; set; } = null!;

    public string EngineDescription { get; set; } = string.Empty;
    public string Engine { get; set; } = null!;
    public string Aspiration { get; set; } = null!;
    public string Intake { get; set; } = null!;
    public string Ignition { get; set; } = null!;
    public string Displacement { get; set; } = null!;
    public string Exhaust { get; set; } = null!;

    public string HandlingDescription { get; set; } = string.Empty;
    public string Brakes { get; set; } = null!;
    public string Suspension { get; set; } = null!;
    public string AntirollBars { get; set; } = null!;
    public string RollCage { get; set; } = null!;

    public string DrivetrainDescription { get; set; } = string.Empty;
    public string Clutch { get; set; } = null!;
    public string Transmission { get; set; } = null!;
    public string Differential { get; set; } = null!;

    public string TiersDescription { get; set; } = string.Empty;
    public string Compound { get; set; } = null!;
    public string FrontTierWidth { get; set; } = null!;
    public string RearTierWidth { get; set; } = null!;
    public string FrontTrackwidth { get; set; } = null!;
    public string RearTrackWidth { get; set; } = null!;
}