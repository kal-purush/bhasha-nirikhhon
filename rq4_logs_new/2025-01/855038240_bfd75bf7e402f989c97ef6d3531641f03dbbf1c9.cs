
using Domain.Interfaces.InfrastructureInterfaces.IObjectLoggers;
using Domain.Interfaces.InfrastructureInterfaces.IStoredSqlFunctionCallers;
using LolMatchFilterNew.Domain.Interfaces.IAppLoggers;
using Domain.DTOs.TeamnameDTOs;
using Domain.Interfaces.ApplicationInterfaces.IMatchDTOServices;
using Domain.DTOs.Western_MatchDTOs;

namespace Application.MatchPairingService.ScoreboardGameService.MatchDTOServices
{
    public class TeamnameService : ITeamnameService
    {
        private readonly IAppLogger _appLogger;
        private readonly IObjectLogger _objectLogger;
        private readonly IStoredSqlFunctionCaller _sqlFunctionCaller;
        private readonly List<WesternMatchDTO> _westernMatches;
        private readonly Dictionary<string, string> _shortNames;
        private readonly Dictionary<string, string> _mediumNames;
        private readonly Dictionary<string, List<string>> _inputs;


        public TeamnameService(IAppLogger appLogger, IObjectLogger objectLogger, IStoredSqlFunctionCaller testFunction)
        {
            _appLogger = appLogger;
            _objectLogger = objectLogger;
            _sqlFunctionCaller = testFunction;


            _westernMatches = _sqlFunctionCaller.GetWesternMatches()
              .GetAwaiter()
              .GetResult();


            _shortNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            _mediumNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            _inputs = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        }


        public void PopulateTeamVariations(IEnumerable<TeamnameDTO> teamNames)
        {
            int count = 0;
            foreach (var team in teamNames)
            {
                if (string.IsNullOrEmpty(team.Longname))
                    continue;

                if (!string.IsNullOrEmpty(team.Short))
                {
                    _shortNames.Add(team.Longname, team.Short);
                }

                if (!string.IsNullOrEmpty(team.Medium))
                {
                    _mediumNames.Add(team.Longname, team.Medium);
                }

                if (team.FormattedInputs != null && team.FormattedInputs.Count > 0)
                {
                    _inputs.Add(team.Longname, team.FormattedInputs);
                }

                count++;
            }

            _appLogger.Info($"{nameof(PopulateTeamVariations)} complete total count: {count}");
        }












    }
}