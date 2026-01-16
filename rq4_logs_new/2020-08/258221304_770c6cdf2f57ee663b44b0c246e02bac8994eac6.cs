using GameSource.Models.GameSourceUser;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace GameSource.Data.Repositories.GameSourceUser.Contracts
{
    public interface IUserProfileCommentRepository
    {
        public IEnumerable<UserProfileComment> GetAll();
        public UserProfileComment GetByID(int id);
        public void Insert(UserProfileComment userProfileComment);
        public void Update(UserProfileComment userProfileComment);
        public void Delete(int id);
        public Task<IEnumerable<UserProfileComment>> GetAllAsync();
        public Task<UserProfileComment> GetByIDAsync(int id);
        public Task<UserProfileComment> InsertAsync(UserProfileComment userProfileComment);
        public Task UpdateAsync(UserProfileComment userProfileComment);
        public Task DeleteAsync(int id);
    }
}