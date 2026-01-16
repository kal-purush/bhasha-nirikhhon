

using Domain.DTOs.TeamnameDTOs;
using Domain.Interfaces.InfrastructureInterfaces.IObjectLoggers;
using LolMatchFilterNew.Domain.Interfaces.IAppLoggers;
using Domain.Interfaces.ApplicationInterfaces.ITeamnameDTOBuilders;
using Domain.Interfaces.ApplicationInterfaces.ITeamNameValidators;

namespace Application.MatchPairingService.YoutubeDataService.TeamNameValidators
{
    public class TeamNameValidator : ITeamNameValidator
    {
        private readonly IAppLogger _appLogger;
        private readonly IObjectLogger _objectLogger;
        private readonly ITeamnameDTOBuilder _teamNameDTOBuilder;
        private readonly List<TeamnameDTO> _TeamNamesAndAbbreviations;
        



        public TeamNameValidator(IAppLogger appLogger, IObjectLogger objectLogger, ITeamnameDTOBuilder teamNameDTOBuilder)
        {
            _appLogger = appLogger;
            _objectLogger = objectLogger;
            _teamNameDTOBuilder = teamNameDTOBuilder;
            _TeamNamesAndAbbreviations = _teamNameDTOBuilder.GetTeamNamesAndAbbreviations();

        }

        public bool ValidateTeamName(string teamName)
        {
            if (VerifyTeamShortName(teamName)) return true;
            if (VerifyTeamMediumName(teamName)) return true;
            if(VerifyFormattedInputs(teamName)) return true;

            return false;
        }




        public bool VerifyTeamShortName(string teamName)
        {
            return _TeamNamesAndAbbreviations.Any(t => string.Equals(t.Short, teamName, StringComparison.OrdinalIgnoreCase));
        }

        public bool VerifyTeamMediumName(string teamName)
        {
            return _TeamNamesAndAbbreviations.Any(t => string.Equals(t.Medium, teamName, StringComparison.OrdinalIgnoreCase));
        }

        public bool VerifyFormattedInputs(string teamName)
        {
            return _TeamNamesAndAbbreviations.Any(t => t.FormattedInputs != null && t.FormattedInputs.Contains(teamName, StringComparer.OrdinalIgnoreCase));
        }

    }
}