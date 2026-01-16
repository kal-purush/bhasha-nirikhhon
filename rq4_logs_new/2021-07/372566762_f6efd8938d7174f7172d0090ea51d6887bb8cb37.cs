using Microsoft.Extensions.Caching.Distributed;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Photos.Infrastructure.Extensions
{
    public static class CacheExtention
    {
        public static async Task SetRecordAsync<T>(this IDistributedCache cache, 
            string recordID, 
            T data,
            TimeSpan? absoluteExpireTime = null,
            TimeSpan? unusedExpiredTime = null)
        {
            var options = new DistributedCacheEntryOptions
            {
                AbsoluteExpirationRelativeToNow = absoluteExpireTime ?? TimeSpan.FromSeconds(60),
                SlidingExpiration = unusedExpiredTime ?? TimeSpan.FromSeconds(60)
            };

            var jsonData = JsonSerializer.Serialize(data);
            await cache.SetStringAsync(recordID, jsonData, options);           
        }

        public static async Task<T> GetRecordAsync<T>(this IDistributedCache cache, string recordID)
        {
            var jsonData = await cache.GetStringAsync(recordID);
            
            if(jsonData == null)
            {
                return default(T);
            }

            return JsonSerializer.Deserialize<T>(jsonData);
        }
    }
}