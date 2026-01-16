using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Caching.Memory;

namespace BookLib.Middleware
{

    public class RequestRateLimitingMiddleware
    {
        public const int Limit = 10;
        private readonly RequestDelegate next;
        private readonly IMemoryCache requestStore;

        public RequestRateLimitingMiddleware(RequestDelegate next, IMemoryCache requestStore)
        {
            this.next = next;
            this.requestStore = requestStore;
        }

        public async Task Invoke(HttpContext context)
        {
            //用method和path作为key，实现了对不同api的限流
            var requestKey = $"{context.Request.Method}-{context.Request.Path}";
            int hitCount = 0;

            //MemoryCacheEntryOptions 中的AbsoluteExpiration定义了该内存缓存过期时间
            var cacheOptions = new MemoryCacheEntryOptions
            {
                AbsoluteExpiration = DateTime.Now.AddMinutes(1)
            };
            if(requestStore.TryGetValue(requestKey, out hitCount))
            {
                if (hitCount < Limit)
                {
                    await ProcessRequest(context, requestKey, hitCount, cacheOptions);
                }
                //当超过限流限制时，返回429 status code
                else
                {
                    context.Response.Headers["X-RateLimit-RetryAfter"] = cacheOptions.AbsoluteExpiration?.ToString();
                    context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
                }
            }
            else
            {
                await ProcessRequest(context, requestKey, hitCount, cacheOptions);
            }
        }

        private async Task ProcessRequest(HttpContext context, string requestKey, int hitCount, MemoryCacheEntryOptions cacheOptions)
        {
            hitCount++;
            //当没有超过限流时，将访问次数存入memory cache，并且加上memory cache配置，主要是为了配置过期时间
            requestStore.Set(requestKey, hitCount, cacheOptions);
            context.Response.Headers["X-RateLimit-Limit"] = Limit.ToString();
            context.Response.Headers["X-RateLimit-Remaining"] = (Limit - hitCount).ToString();
            await next(context);
        }
    }
}