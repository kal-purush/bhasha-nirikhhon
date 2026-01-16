using System.Threading.Tasks;
using Sibur.Learn.DotNet.Solid.Models;

namespace Sibur.Learn.DotNet.Solid.BusinessLogic.Users
{
    /// <summary>
    /// TODO: 01. Какая ответственность этого класса?
    /// </summary>
    public interface IUserService
    {
        Task<User> GetUserAsync(int id);
        Task CreateAsync(User newUser);
        Task Lock(int id);
    }
}