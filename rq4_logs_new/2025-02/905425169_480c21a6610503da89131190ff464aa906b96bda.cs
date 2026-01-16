using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Circle.Data.Models;

namespace Circle.Data.Repositories
{
    public class FriendshipRepository : IFriendshipRepository
    {
        protected readonly CircleDbContext _dbContext;

        public FriendshipRepository(CircleDbContext dbContext)
        {
            this._dbContext = dbContext;
        }

        public async Task<Friendship> AddFriendAsync(Guid userId, Guid friendId)
        {
            if (await AreFriendsAsync(userId, friendId))
            {
                throw new InvalidOperationException("Users are already friends.");
            }

            var friendship = new Friendship
            {
                UserId = userId,
                FriendId = friendId
            };

            await this._dbContext.Friendships.AddAsync(friendship);
            await this._dbContext.SaveChangesAsync();
            return friendship;
        }

        public async Task RemoveFriendAsync(Guid userId, Guid friendId)
        {
            var friendship = await this._dbContext.Friendships
                .SingleOrDefaultAsync(f => f.UserId == userId && f.FriendId == friendId);

            if (friendship != null)
            {
                this._dbContext.Friendships.Remove(friendship);
                await this._dbContext.SaveChangesAsync();
            }
        }

        public async Task<bool> AreFriendsAsync(Guid userId, Guid friendId)
        {
            return await this._dbContext.Friendships.AnyAsync(f => f.UserId == userId && f.FriendId == friendId);
        }

        public IQueryable<Friendship> GetAllFriendships()
        {
            return this._dbContext.Friendships.AsQueryable();
        }
    }
}