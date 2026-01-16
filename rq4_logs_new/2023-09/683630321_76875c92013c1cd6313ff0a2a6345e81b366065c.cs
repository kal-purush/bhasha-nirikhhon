using Common.Settings;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;

namespace Web.User.Helpers
{
    public class CacheHelper
    {
        private readonly IMemoryCache memoryCache;
        private readonly AppSettings appSettings;
        
        public CacheHelper(IMemoryCache memoryCache, IOptions<AppSettings> options)
        {
            this.memoryCache = memoryCache;            
            appSettings=options.Value;
        }

        public void Set<T>(string key, T value)
        {
            var entryOptions = new MemoryCacheEntryOptions();
            entryOptions.SetSlidingExpiration(TimeSpan.FromMinutes(appSettings.Cache.TimeoutInMinutes));
            memoryCache.Set<T>(key, value, entryOptions);
        }

        public T Get<T>(string key)
        {
            memoryCache.TryGetValue<T>(key, out var value);
            return value;
        }

        public void Remove(string key) 
        {
            memoryCache.TryGetValue(key, out var value);
            if (value != null)
            {
                memoryCache.Remove(key);
            }
        }
    }
}