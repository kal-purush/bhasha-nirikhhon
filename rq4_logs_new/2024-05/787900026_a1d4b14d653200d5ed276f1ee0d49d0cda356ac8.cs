using Microsoft.Extensions.Caching.Distributed;
using MovieAPI.Application.Interfaces;
using System.Text.Json;

namespace MovieAPI.Application.Services;

public class CacheService : ICacheService
{
    private readonly IDistributedCache _distributedCache;

    public CacheService(IDistributedCache distributedCache)
    {
        _distributedCache = distributedCache;
    }

    public async Task<string> GetStringAsync(string key)
    {
        return await _distributedCache.GetStringAsync(key);
    }

    public async Task SetStringAsync(string key, string value, DistributedCacheEntryOptions options)
    {
        await _distributedCache.SetStringAsync(key, value, options);
    }

    public async Task<byte[]> GetAsync(string key)
    {
        return await _distributedCache.GetAsync(key);
    }

    public async Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options)
    {
        await _distributedCache.SetAsync(key, value, options);
    }

    public async Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valueFactory, DistributedCacheEntryOptions options)
    {
        var cachedValue = await _distributedCache.GetAsync(key);
        if (cachedValue != null)
        {
            return JsonSerializer.Deserialize<T>(cachedValue);
        }

        var newValue = await valueFactory();
        await SetAsync(key, JsonSerializer.SerializeToUtf8Bytes(newValue), options);
        return newValue;
    }
}