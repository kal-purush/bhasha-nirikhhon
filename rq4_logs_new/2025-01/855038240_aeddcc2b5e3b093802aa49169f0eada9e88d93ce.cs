
using Domain.Interfaces.InfrastructureInterfaces.IObjectLoggers;
using Domain.Interfaces.InfrastructureInterfaces.IStoredSqlFunctionCallers;
using LolMatchFilterNew.Domain.Interfaces.IAppLoggers;
using Domain.DTOs.TeamnameDTOs;
using Domain.Interfaces.ApplicationInterfaces.IMatchDTOServices;

namespace Application.MatchPairingService.ScoreboardGameService.MatchDTOServices
{
    public class MatchDTOService : IMatchDTOService
    {
        private readonly IAppLogger _appLogger;
        private readonly IObjectLogger _objectLogger;
        private readonly IStoredSqlFunctionCaller _testFunction;


        public MatchDTOService(IAppLogger appLogger, IObjectLogger objectLogger, IStoredSqlFunctionCaller testFunction)
        {
            _appLogger = appLogger;
            _objectLogger = objectLogger;
            _testFunction = testFunction;
        }



        public async Task<bool> CheckYoutubeTeamnameValid()
        {

        }



    }
}