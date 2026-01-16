using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DfE.EmployerFavourites.Web.Domain;
using MediatR;
using Microsoft.Extensions.Logging;
using SFA.DAS.EAS.Account.Api.Client;

namespace DfE.EmployerFavourites.Web.Commands
{
    public class SaveApprenticeshipFavouriteCommandHandler : AsyncRequestHandler<SaveApprenticeshipFavouriteCommand>
    {
        private readonly ILogger<SaveApprenticeshipFavouriteCommandHandler> _logger;
        private readonly IFavouritesRepository _repository;
        private readonly IAccountApiClient _accountApiClient;

        public SaveApprenticeshipFavouriteCommandHandler(
            ILogger<SaveApprenticeshipFavouriteCommandHandler> logger,
            IFavouritesRepository repository, 
            IAccountApiClient accountApiClient)
        {
            _logger = logger;
            _repository = repository;
            _accountApiClient = accountApiClient;
        }

        protected override async Task Handle(SaveApprenticeshipFavouriteCommand request, CancellationToken cancellationToken)
        {
            var employerAccountId = await GetEmployerAccountId(request.UserId);
            var favourites = (await _repository.GetApprenticeshipFavourites(employerAccountId)) ?? new ApprenticeshipFavourites();

            var existing = favourites.SingleOrDefault(x => x.ApprenticeshipId == request.ApprenticeshipId);

            if (existing == null)
            {
                if (request.Ukprn.HasValue)
                    favourites.Add(new ApprenticeshipFavourite(request.ApprenticeshipId, request.Ukprn.Value));
                else
                    favourites.Add(new ApprenticeshipFavourite(request.ApprenticeshipId));
            }
            else
            {
                if (!request.Ukprn.HasValue)
                    return;
                else if (existing.Ukprns.Contains(request.Ukprn.Value))
                    return;
                else    
                    existing.Ukprns.Add(request.Ukprn.Value);
            }

            await _repository.SaveApprenticeshipFavourites(employerAccountId, favourites);
        }

        private async Task<string> GetEmployerAccountId(string userId)
        {
            try
            {
                var accounts = await _accountApiClient.GetUserAccounts(userId);
                
                return accounts.First().HashedAccountId;

                    // Currenly the API isn't passing through the correct details
                    // of the registered date
                    // We may be able to rely on the order from the api. They do 
                    // look like they're returned in the order they are created.
                    // .OrderBy(x => x.DateRegistered)
                    // .ThenBy(x => x.HashedAccountId)
                    // .First().HashedAccountId;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Failed to retrieve account information for user Id: {userId}");
                throw;
            }
        }
    }
}