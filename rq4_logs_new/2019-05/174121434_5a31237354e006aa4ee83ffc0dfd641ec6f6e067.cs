using System;
using System.Threading;
using System.Threading.Tasks;
using DfE.EmployerFavourites.ApplicationServices.Domain;
using MediatR;
using Microsoft.Extensions.Logging;
using SFA.DAS.EAS.Account.Api.Client;

namespace DfE.EmployerFavourites.ApplicationServices.Commands
{
    public class GetApprenticeshipFavouriteRequestHandler : IRequestHandler<GetApprenticeshipFavouritesRequest,ApprenticeshipFavourites>
    {
        private readonly ILogger<GetApprenticeshipFavouriteRequestHandler> _logger;
        private readonly IFavouritesRepository _repository;
        private readonly IEmployerAccountRepository _employerAccountRepository;

        public GetApprenticeshipFavouriteRequestHandler(
            ILogger<GetApprenticeshipFavouriteRequestHandler> logger,
            IFavouritesRepository repository, IEmployerAccountRepository employerAccountRepository)
        {
            _logger = logger;
            _repository = repository;
            _employerAccountRepository = employerAccountRepository;
        }

        public async Task<ApprenticeshipFavourites> Handle(GetApprenticeshipFavouritesRequest request, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(request.UserId))
            {
                throw new ArgumentException("User Id is required but is not provided");
            }

            var employerAccountId = await _employerAccountRepository.GetEmployerAccountId(request.UserId);
            var favourites = (await _repository.GetApprenticeshipFavourites(employerAccountId)) ?? new ApprenticeshipFavourites();

            return favourites;
        }
    }
}