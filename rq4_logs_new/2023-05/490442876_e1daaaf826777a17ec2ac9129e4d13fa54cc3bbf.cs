using GiftSuggester.Core.Gifts.Models;

namespace GiftSuggester.Core.Gifts.Services;

public interface IGiftService
{
    Task AddAsync(GiftCreationModel creationModel, CancellationToken cancellationToken);
    Task<List<Gift>> GetAllByPresenterIdAsync(Guid id, CancellationToken cancellationToken);
    Task<List<Gift>> GetAllByRecipientIdAsync(Guid id, CancellationToken cancellationToken);
    Task UpdateAsync(Gift gift, CancellationToken cancellationToken);
    Task RemoveByIdAsync(Guid id, CancellationToken cancellationToken);
}