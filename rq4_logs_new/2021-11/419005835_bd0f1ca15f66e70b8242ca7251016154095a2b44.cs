using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using SchedulerViewModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BLL.Caching
{
    public class MemoryUserCacheService : IUserCacheService
    {
        private const int EXPIRATION_TIME_MINUTES = 5;

        private readonly IMemoryCache _cache;
        public MemoryUserCacheService(IMemoryCache cache)
        {
            _cache = cache; 
        }
        public StudentViewModel GetUserFromCache(Guid userId)
        {
            StudentViewModel user;
            _cache.TryGetValue(userId, out user);
            return user;
        }

        public void SetUserToCache(StudentViewModel user)
        {
            _cache.Set(user.Id, user, new MemoryCacheEntryOptions().SetSlidingExpiration(TimeSpan.FromMinutes(EXPIRATION_TIME_MINUTES)));
        }
    }
}