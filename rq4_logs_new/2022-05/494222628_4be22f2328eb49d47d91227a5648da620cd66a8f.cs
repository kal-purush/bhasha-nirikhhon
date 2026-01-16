using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Models;
namespace BL
{
    public interface IUserLogic
    {
        User AddUser(User user);
        List<User> GetAllUsers();
        User GetUserByName(string name);
        User Update(User user);
        void DeleteUser(int id);
    }
}