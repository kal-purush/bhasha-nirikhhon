namespace Basisregisters.IntegrationDb.Reporting.SuspiciousCases.Infrastructure;

using System.Collections.Generic;
using Dapper;
using Npgsql;

public interface IMunicipalityRepository
{
    IEnumerable<MunicipalityDto> GetAll();
}

public class MunicipalityRepository : IMunicipalityRepository
{
    private readonly string _connectionString;

    public MunicipalityRepository(string connectionString)
    {
        _connectionString = connectionString;
    }

    public IEnumerable<MunicipalityDto> GetAll()
    {
        const string sql = """
                            select
                                municipality.nis_code as NisCode
                                , municipality.name_dutch as DutchName
                            from integration_municipality.municipality_latest_items municipality
                           """;

        using var connection = new NpgsqlConnection(_connectionString);

        var municipalities = connection.Query<MunicipalityDto>(sql);

        return municipalities;
    }
}

public class MunicipalityDto
{
    public string NisCode { get; init; } = null!;
    public string DutchName { get; init; } = null!;

    // Needed for dapper
    protected MunicipalityDto()
    { }

    public MunicipalityDto(
        string nisCode,
        string dutchName)
    {
        NisCode = nisCode;
        DutchName = dutchName;
    }
}