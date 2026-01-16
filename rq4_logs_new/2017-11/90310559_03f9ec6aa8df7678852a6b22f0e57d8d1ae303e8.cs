using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;

namespace ToDoNetCore.Controllers
{
    public class MemoryCacheController : Controller
    {
        private readonly IMemoryCache memoryCache;

        public MemoryCacheController(IMemoryCache memoryCache)
        {
            this.memoryCache = memoryCache;
        }
        public async Task<IActionResult> ServerCached()
        {
            var cacheEntry = await
                memoryCache.GetOrCreateAsync("time", entry =>
            {
                entry.AbsoluteExpirationRelativeToNow = TimeSpan.FromSeconds(30);
                return Task.FromResult(DateTime.Now);
            });
            ViewBag.CachedTime = cacheEntry.ToString();
            return View();
        }
    }
}