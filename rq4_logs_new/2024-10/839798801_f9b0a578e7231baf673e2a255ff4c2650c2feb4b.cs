namespace PatientAnalytics.Models.PatientMetrics.Units;

public enum HeightUnit
{
    Cm,
    In,
    Ft,
}

internal static class HeightUnitMethods
{
    public static string ToLabel(this HeightUnit heightUnit)
    {
        return heightUnit switch
        {
            HeightUnit.Cm => "Unit_Cm",
            HeightUnit.In => "Unit_In",
            HeightUnit.Ft => "Unit_Ft",
            _ => ""
        };
    }

    public static string? SelectEntryValueForUnit(this HeightUnit heightUnit, PatientHeight entry)
    {
        return heightUnit switch
        {
            HeightUnit.Cm => entry.HeightCmFormatted,
            HeightUnit.In => entry.HeightInFormatted,
            HeightUnit.Ft => entry.HeightFtFormatted,
            _ => null
        };
    }
}