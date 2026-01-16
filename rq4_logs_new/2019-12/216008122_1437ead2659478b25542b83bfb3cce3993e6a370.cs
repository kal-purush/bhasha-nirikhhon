using DB.Api.Application.Queries;
using DB.Core.Interfaces;
using MediatR;
using System.Threading;
using System.Threading.Tasks;

namespace DB.Api.Application.QueryHandlers
{
    public class GetAvailabilityUsernameQueryHandler : IRequestHandler<GetAvailabilityUsernameQuery, bool>
    {
        private readonly IUserRepository _userRepository;

        public GetAvailabilityUsernameQueryHandler(IUserRepository userRepository) =>
            _userRepository = userRepository;

        public Task<bool> Handle(GetAvailabilityUsernameQuery query, CancellationToken cancellationToken) =>
            _userRepository.AvailabilityUsernameAsync(query.Username, cancellationToken);
    }
}