using SQLite;
using Xamarin.Forms;

using NotifyMe.Models;
using NotifyMe.ServiceInterfaces;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NotifyMe.Services
{
    public class UserService : IUserService
    {
        private SQLiteConnection _dbContext;

        public UserService()
        {
            _dbContext = DependencyService.Get<ISqliteConnection>().GetConnection();
            _dbContext.CreateTable<User>();
        }

        public bool AddUserAsync(User user)
        {
            throw new System.NotImplementedException();
        }

        public IEnumerable<User> GetAllUsersAsync()
        {
            throw new System.NotImplementedException();
        }

        public User GetUserByEmailAsync(string Email)
        {
            var user = _dbContext.Table<User>().FirstOrDefault(u => u.Email == Email);
            return user;
        }
    }
}