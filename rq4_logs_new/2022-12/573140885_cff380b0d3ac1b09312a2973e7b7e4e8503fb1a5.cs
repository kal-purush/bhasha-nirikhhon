namespace Services;

public static class CodesService
{
    public static int GetCfps(int baseCode, int? cityCode)
    {
        Dictionary<string, int> CfpsTable = new()
            {
                { "SameCity", 9201 },
                { "SameState", 9202 },
                { "OtherState", 9203 },
                { "OtherCountry", 9204 },
            };

        if (cityCode is null)
        {
            return CfpsTable["OtherCountry"];
        }

        if (cityCode == baseCode)
        {
            return CfpsTable["SameCity"];
        }

        string baseStateCode = $"{baseCode}";
        string cityStateCode = $"{cityCode}";

        bool hasSameStateBase = baseStateCode.Substring(0, 2) == cityStateCode.Substring(0, 2);

        if (hasSameStateBase)
        {
            return CfpsTable["SameState"];
        }

        return CfpsTable["OtherState"];
    }
    public static string GetCountry(int? countryCode)
    {
        return "Brasil";
    }

    public static string GetCity(int? cityCode)
    {
        return cityCode switch
        {
            3550308 => "São Paulo",
            4205407 => "Florianópolis",
            _ => String.Empty,
        };
    }
}