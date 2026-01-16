using DfE.EmployerFavourites.ApplicationServices.Domain;
using SFA.DAS.Apprenticeships.Api.Client;

namespace DfE.EmployerFavourites.ApplicationServices.Infrastructure
{
    public class FatApiRepository : IFatRepository
    {
        private readonly IStandardApiClient _standardApiClient;
        private readonly IFrameworkApiClient _frameworkApiClient;

        public FatApiRepository(IStandardApiClient standardApiClient, IFrameworkApiClient frameworkApiClient)
        {
            _standardApiClient = standardApiClient;
            _frameworkApiClient = frameworkApiClient;
        }

        public string GetApprenticeshipName(string apprenticeshipId)
        {
            if (isStandard(apprenticeshipId))
            {
                return _standardApiClient.Get(apprenticeshipId).Title;
            }
            else
            {
                return _frameworkApiClient.Get(apprenticeshipId).Title;
            }
        }

        private bool isStandard(string apprenticeshipId)
        {
            int standardId;
            return int.TryParse(apprenticeshipId, out standardId);
        }
    }
}