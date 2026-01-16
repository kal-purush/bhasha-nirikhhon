using CardStorageService.Core.Entities;

namespace CardStorageService.Abstractions.Interfaces.Repositories
{
    public interface ICardRepositoryService : IRepository<Card, Guid>
    {
        Task<IList<Card>> GetByClientIdAsync(Guid id);
    }
}