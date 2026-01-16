using EightCare.API.Models;
using EightCare.API.Queries;
using EightCare.Domain.KeeperAggregate.Abstractions;
using MediatR;
using System.Threading;
using System.Threading.Tasks;

namespace EightCare.API.QueryHandlers
{
    public class GetKeeperByIdQueryHandler : IRequestHandler<GetKeeperByIdQuery, KeeperModel>
    {
        private readonly IKeeperRepository _keeperRepository;

        public GetKeeperByIdQueryHandler(IKeeperRepository keeperRepository)
        {
            _keeperRepository = keeperRepository;
        }

        public Task<KeeperModel> Handle(GetKeeperByIdQuery request, CancellationToken cancellationToken)
        {
            var keeper = _keeperRepository.GetById(request.KeeperId);

            return Task.FromResult(new KeeperModel
            {
                Id = keeper.Id,
                Name = keeper.Name,
                Email = keeper.Email,
                Age = keeper.Age
            });
        }
    }
}