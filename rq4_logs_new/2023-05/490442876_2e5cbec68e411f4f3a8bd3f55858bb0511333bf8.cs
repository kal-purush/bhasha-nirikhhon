using GiftSuggester.Core.Gifts.Models;

namespace GiftSuggester.Core.Gifts.Repositories;

public interface IGiftRepository
{
    Task AddAsync(Gift gift, CancellationToken cancellationToken);
    Task<List<Gift>> GetAllByPresenterIdAsync(Guid id, CancellationToken cancellationToken);
    Task<List<Gift>> GetAllByRecipientIdAsync(Guid id, CancellationToken cancellationToken);
    Task UpdateAsync(Gift gift, CancellationToken cancellationToken);
    Task RemoveByIdAsync(Guid id, CancellationToken cancellationToken);
}