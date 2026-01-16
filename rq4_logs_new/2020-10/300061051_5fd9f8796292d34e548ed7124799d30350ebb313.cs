using EightCare.Domain.Common;
using EightCare.Domain.KeeperAggregate;
using EightCare.Domain.KeeperAggregate.Abstractions;
using System;
using System.Linq;

namespace EightCare.Infrastructure.Repositories
{
    public sealed class InMemoryKeeperRepository : IKeeperRepository
    {
        private readonly IKeeperUnitOfWork _unitOfWork;

        public IUnitOfWork UnitOfWork => _unitOfWork;

        public InMemoryKeeperRepository(IKeeperUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        public Keeper GetById(Guid keeperId)
        {
            return _unitOfWork.Keepers.FirstOrDefault(x => x.Id == keeperId);
        }

        public void Add(Keeper keeper)
        {
            keeper.SetId(Guid.Empty);

            _unitOfWork.Keepers.Add(keeper);
        }

        public void Update(Keeper keeper)
        {
            if (keeper.Id == Guid.Empty)
                throw new ArgumentException("Keeper without Id cannot be updated.");

            _unitOfWork.Keepers.Remove(keeper);
            _unitOfWork.Keepers.Add(keeper);
        }

        public void Delete(Keeper keeper)
        {
            if (keeper.Id == Guid.Empty)
                throw new ArgumentException("Keeper without Id cannot be removed.");

            _unitOfWork.Keepers.Remove(keeper);
        }
    }
}