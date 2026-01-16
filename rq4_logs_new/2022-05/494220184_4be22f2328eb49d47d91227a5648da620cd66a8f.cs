using DL;
using Models;

namespace BL
{
    public class NewUserLogic : IUserLogic
    {
        readonly IUserRepoNew repo;

        public NewUserLogic(IUserRepoNew repo)
        {
            this.repo = repo;
        }
        public User AddUser(User user)
        {
            List<User>? users = repo.GetAllUsers();
            return repo.AddUser(user);
        }

        public void DeleteUser(int id)
        {
            throw new NotImplementedException();
        }

        public List<User> GetAllUsers()
        {
            throw new NotImplementedException();
        }

        public User GetUserByName(string name)
        {
            throw new NotImplementedException();
        }

        public User Update(User user)
        {
            throw new NotImplementedException();
        }
    }
}