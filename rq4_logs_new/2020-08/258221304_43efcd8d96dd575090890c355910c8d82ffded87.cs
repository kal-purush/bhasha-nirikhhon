using GameSource.Models.GameSourceUser;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace GameSource.Data.Repositories.GameSourceUser.Contracts
{
    public interface IUserProfileRepository
    {
        public IEnumerable<UserProfile> GetAll();
        public UserProfile GetByID(int id);
        public void Insert(UserProfile userProfile);
        public void Update(UserProfile userProfile);
        public void Delete(int id);
        public Task<IEnumerable<UserProfile>> GetAllAsync();
        public Task<UserProfile> GetByIDAsync(int id);
        public Task<UserProfile> InsertAsync(UserProfile userProfile);
        public Task UpdateAsync(UserProfile userProfile);
        public Task DeleteAsync(int id);
    }
}