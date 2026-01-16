using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;
using SFA.DAS.SecureMessageService.Core.IRepositories;

namespace SFA.DAS.SecureMessageService.Infrastructure.Repositories
{
    public class DasDistributedCache : IDasDistributedCache
    {
        private readonly IDistributedCache _cache;

        public DasDistributedCache(IDistributedCache cache)
        {
            _cache = cache;
        }

        public async Task SetStringAsync(string key, string message, DistributedCacheEntryOptions distributedCacheEntryOptions)
        {
            await _cache.SetStringAsync(key, message, distributedCacheEntryOptions);
        }
        
        public async Task<string> GetStringAsync(string key)
        {
            var value = await _cache.GetStringAsync(key);
            return value;
        }

        public async Task RemoveAsync(string key)
        {
            await _cache.RemoveAsync(key);
        }
    }
}