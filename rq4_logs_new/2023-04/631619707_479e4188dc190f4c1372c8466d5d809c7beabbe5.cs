namespace PocketForzaHorizonComunity.Back.DTO.DTOs.GuidesDtos;

public class TuneFullInfoDto : TuneDto
{
    public string EngineDescription { get; set; } = string.Empty;
    public string Engine { get; set; } = string.Empty;
    public string Aspiration { get; set; } = string.Empty;
    public string Intake { get; set; } = string.Empty;
    public string Ignition { get; set; } = string.Empty;
    public string Displacement { get; set; } = string.Empty;
    public string Exhaust { get; set; } = string.Empty;

    public string HandlingDescription { get; set; } = string.Empty;
    public string Brakes { get; set; } = string.Empty;
    public string Suspension { get; set; } = string.Empty;
    public string AntirollBars { get; set; } = string.Empty;
    public string RollCage { get; set; } = string.Empty;

    public string DrivetrainDescription { get; set; } = string.Empty;
    public string Clutch { get; set; } = string.Empty;
    public string Transmission { get; set; } = string.Empty;
    public string Differential { get; set; } = string.Empty;

    public string TiersDescription { get; set; } = string.Empty;
    public string Compound { get; set; } = string.Empty;
    public string FrontTierWidth { get; set; } = string.Empty;
    public string RearTierWidth { get; set; } = string.Empty;
    public string FrontTrackwidth { get; set; } = string.Empty;
    public string RearTrackWidth { get; set; } = string.Empty;
}