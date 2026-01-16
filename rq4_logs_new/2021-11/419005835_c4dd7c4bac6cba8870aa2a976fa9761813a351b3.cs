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
    public class UserCacheService : MemoryCache, IUserCacheService
    {
        private const int EXPIRATION_TIME_MINUTES = 5;
        public UserCacheService(IOptions<MemoryCacheOptions> options) : base(options)
        {
            
        }
        public StudentViewModel GetUserFromCache(Guid userId)
        {
            StudentViewModel user;
            this.TryGetValue(userId, out user);
            return user;
        }

        public StudentViewModel SetUserToCache(StudentViewModel user)
        {
            this.Set(user.Id, user, new MemoryCacheEntryOptions().SetSlidingExpiration(TimeSpan.FromMinutes(EXPIRATION_TIME_MINUTES)));
            return user;
        }
    }
}