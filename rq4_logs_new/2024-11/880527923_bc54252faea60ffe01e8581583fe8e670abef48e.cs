using Microsoft.AspNetCore.Mvc;
using MonezyAPI.Models;

namespace MonezyAPI.Interfaces
{
    public interface IUserRepository
    {
        public ActionResult<List<Users>> GetUsers();

        public Task<ActionResult<Users>> GetUserById(int IdUser);

        public Task<ActionResult<Users>> InsertUsers(Users user);

        public Task<Users?> PatchUser(Users user);

        public Task DeleteUser(int IdUser);
    }
}