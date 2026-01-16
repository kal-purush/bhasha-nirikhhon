

// Ignore Spelling: Youtube

using Domain.DTOs.TeamnameDTOs;
using Domain.DTOs.Western_MatchDTOs;
using Domain.Interfaces.ApplicationInterfaces.IMatchDTOServices.IImport_TeamNameServices;
using Domain.Interfaces.ApplicationInterfaces.ITeamNameDTOBuilders;
using Domain.Interfaces.InfrastructureInterfaces.IImport_TeamnameRepositories;
using Domain.Interfaces.InfrastructureInterfaces.IObjectLoggers;
using Domain.Interfaces.InfrastructureInterfaces.IStoredSqlFunctionCallers;
using LolMatchFilterNew.Domain.Interfaces.IAppLoggers;

namespace Application.MatchPairingService.ScoreboardGameService.MatchDTOServices.TeamNameServices.Import_TeamNameServices;

public class Import_TeamNameService : IImport_TeamNameService
{
    private readonly IAppLogger _appLogger;
    private readonly IObjectLogger _objectLogger;
    private readonly IStoredSqlFunctionCaller _sqlFunctionCaller;
    private readonly IImport_TeamnameRepository _teamnameRepository;
    private readonly ITeamNameDTOBuilder _teamNameDTOBuilder;

    public List<TeamNameDTO> Import_TeamNameAllNames = new List<TeamNameDTO>();




    public Import_TeamNameService(IAppLogger appLogger, IObjectLogger objectLogger, IStoredSqlFunctionCaller testFunction, IImport_TeamnameRepository teamNameRepository, ITeamNameDTOBuilder teamNameDTOBuilder)
    {

        _appLogger = appLogger;
        _objectLogger = objectLogger;
        _sqlFunctionCaller = testFunction;
        _teamnameRepository = teamNameRepository;
        _teamNameDTOBuilder = teamNameDTOBuilder;

    }


    public List<TeamNameDTO> ReturnImport_TeamNameAllNames()
    {
        if (Import_TeamNameAllNames == null || !Import_TeamNameAllNames.Any())
        {
            throw new ArgumentNullException(nameof(Import_TeamNameAllNames), "The list is null or empty.");
        }
        return Import_TeamNameAllNames;
    }


    // Retrieves all team names from repository and transforms them into DTOs, storing them in Import_TeamNameAllNames property.
    // Inputs in database need to be trimmed & formatted correctly to remove quotation marks etc, example: {"1 trick ponies;1tp"}
    public async Task PopulateImport_TeamNameAllNames()
    {
        var teamnames = await _teamnameRepository.GetAllTeamnamesAsync();
        Import_TeamNameAllNames = teamnames.Select(t => _teamNameDTOBuilder.BuildTeamnameDTO(
            t.TeamnameId,
            t.Longname,
            t.Short,
            t.Medium,
            t.Inputs

        )).ToList();

        _appLogger.Info($"Import_TeamNameAllNames count: {Import_TeamNameAllNames.Count}");
    }









    public void TESTCheckAllProcessedEuAndNaAgainstKnownAbbreviations(HashSet<string> distinctTeamNames, List<TeamNameDTO> teamNameDTOs)
    {
        int matchCount = 0;
        int missCount = 0;

        foreach (string teamName in distinctTeamNames)
        {
            bool foundMatch = false;

            foreach (var dto in teamNameDTOs)
            {
                if (IsTeamNameMatch(teamName, dto))
                {
                    matchCount++;
                    foundMatch = true;
                    break;
                }
            }

            if (!foundMatch)
            {
                missCount++;
                _appLogger.Info($"No match found for team: {teamName}");
            }
        }

        _appLogger.Info($"Total matches: {matchCount}, Total misses: {missCount}");
    }



    private bool IsTeamNameMatch(string teamName, TeamNameDTO dto)
    {
        if (!string.IsNullOrEmpty(dto.LongName) && teamName.Equals(dto.LongName, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (!string.IsNullOrEmpty(dto.Medium) && teamName.Equals(dto.Medium, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (!string.IsNullOrEmpty(dto.Short) && teamName.Equals(dto.Short, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (dto.FormattedInputs?.Contains(teamName, StringComparer.OrdinalIgnoreCase) == true)
        {
            return true;
        }

        return false;
    }




    public async Task TESTLogTeamNameAbbreviations()
    {
        await PopulateImport_TeamNameAllNames();

        var firstTeamDTO = Import_TeamNameAllNames.FirstOrDefault();


        var lastTeamDTO = Import_TeamNameAllNames.LastOrDefault();

        _objectLogger.LogTeamnameDTO(firstTeamDTO);
        _objectLogger.LogTeamnameDTO(lastTeamDTO);

    }
}












