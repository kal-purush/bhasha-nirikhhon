namespace Crypto.Infrastructure.Persistence.Configurations;

public record DbInstanceInfo(string Schema, string Name)
{
    public string FullName => $"{Schema}.{Name}";
}

public static class DbTables
{
    public static readonly DbInstanceInfo CryptoTable = new("public", "crypto");
    public static readonly DbInstanceInfo CryptoPriceTable = new("public", "crypto_price");
    public static readonly DbInstanceInfo VisitTable = new("public", "visit");
}

public static class DbFunctions
{
    public static readonly DbInstanceInfo CryptoPriceTimeFrame = new("public", "get_data_by_timeframe");
}