using Crud.Example.Main.Auth.Models;
using Microsoft.AspNetCore.Identity;

namespace Crud.Example.Main.Auth.Interfaces
{
    public interface IAuthService
    {
        Task<TokenModel?> LoginUser(LoginModel model);
        Task<IdentityResult> Registeruser(RegisterModel model);
        Task<bool> UserExist(string username);
        bool ValidateToken(string token);
    }
}