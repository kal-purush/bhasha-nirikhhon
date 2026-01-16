using EightCare.API.Commands;
using EightCare.Domain.KeeperAggregate;
using EightCare.Domain.KeeperAggregate.Abstractions;
using MediatR;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace EightCare.API.CommandHandlers
{
    public class RegisterKeeperCommandHandler : IRequestHandler<RegisterKeeperCommand, Guid>
    {
        private readonly IKeeperRepository _keeperRepository;

        public RegisterKeeperCommandHandler(IKeeperRepository keeperRepository)
        {
            _keeperRepository = keeperRepository;
        }

        public Task<Guid> Handle(RegisterKeeperCommand request, CancellationToken cancellationToken)
        {
            var keeper = new Keeper(request.Name, request.Email, request.Age);

            _keeperRepository.Add(keeper);
            _keeperRepository.UnitOfWork.SaveChanges();

            return Task.FromResult(keeper.Id);
        }
    }
}