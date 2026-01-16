namespace ShareInvest.Entities.Google.Maps;

public class MapStock
{
    public double Longitude
    {
        get; set;
    }
    public double Latitude
    {
        get; set;
    }
    public double Rate
    {
        get; set;
    }
    public int CompareToPreviousDay
    {
        get; set;
    }
    public int Current
    {
        get; set;
    }
    public string? Code
    {
        get; set;
    }
    public string? Name
    {
        get; set;
    }
    public string? CompareToPreviousSign
    {
        get; set;
    }
    public bool IsRendering
    {
        get; set;
    }
}