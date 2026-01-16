using ABU.GitHubCommunicationService.Core.Data.Models;

namespace ABU.GitHubCommunicationService.Infrastructure.Abstractions
{
    public interface IGitHubApiService
    {
        Task<IReadOnlyList<Repository>> GetUserRepositoriesFromApiAsync(CancellationToken ct = default);
    }
}