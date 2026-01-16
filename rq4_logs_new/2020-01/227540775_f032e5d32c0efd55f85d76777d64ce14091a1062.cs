using System.Threading;
using System.Threading.Tasks;
using Dfe.Spi.UkrlpAdapter.Domain.UkrlpApi;

namespace Dfe.Spi.UkrlpAdapter.Domain.Cache
{
    public interface IProviderRepository
    {
        Task StoreAsync(Provider provider, CancellationToken cancellationToken);
        Task StoreInStagingAsync(Provider[] providers, CancellationToken cancellationToken);
        Task<Provider> GetProviderAsync(long ukprn, CancellationToken cancellationToken);
        Task<Provider> GetProviderFromStagingAsync(long ukprn, CancellationToken cancellationToken);
    }
}