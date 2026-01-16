using Domain.Interfaces.InfrastructureInterfaces.IImportRepositories.IImport_TournamentRepositories;
using Domain.Interfaces.InfrastructureInterfaces.IObjectLoggers;
using Domain.Interfaces.InfrastructureInterfaces.IStoredSqlFunctionCallers;
using LolMatchFilterNew.Domain.Interfaces.IAppLoggers;
using LolMatchFilterNew.Domain.Interfaces.InfrastructureInterfaces.IImport_YoutubeDataRepositories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

/*
EU
League of Legends Championship Series
North America League Championship Series
LoL EMEA Championship
League of Legends Championship of The Americas North
League of Legends Championship of The Americas South
League of Legends Championship of The Americas
Europe League Championship Series
*/



namespace Application.MatchPairingService.TournamentService.YoutubeTournamentMatchers
{
    public class TournamentDurationCalculator
    {
        private readonly IAppLogger _appLogger;
        private readonly IObjectLogger _objectLogger;
        private readonly IImport_TournamentRepository _importTournamentRepository;
        private readonly IImport_YoutubeDataRepository _import_YoutubeDataRepository;
        private readonly IStoredSqlFunctionCaller _storedSqlFunctionCaller;


        public TournamentDurationCalculator(IAppLogger appLogger, IObjectLogger objectLogger, IImport_TournamentRepository importTournamentRepository, IImport_YoutubeDataRepository import_YoutubeDataRepository, IStoredSqlFunctionCaller storedSqlFunctionCaller)
        {
            _appLogger = appLogger;
            _objectLogger = objectLogger;
            _importTournamentRepository = importTournamentRepository;
            _import_YoutubeDataRepository = import_YoutubeDataRepository;
            _storedSqlFunctionCaller = storedSqlFunctionCaller;
        }









    }
}