using System.Threading.Tasks;
using CoolBytes.Core.Interfaces;
using CoolBytes.Core.Models;
using CoolBytes.Data;
using Microsoft.EntityFrameworkCore;

namespace CoolBytes.WebAPI.Services
{
    public class AuthorValidator : IAuthorValidator
    {
        private readonly AppDbContext _appDbContext;

        public AuthorValidator(AppDbContext appDbContext)
        {
            _appDbContext = appDbContext;
        }

        public async Task<bool> Exists(User user) => await FoundAnyAuthor(user);

        public async Task<bool> Exists(IUserService userService)
        {
            var user = await userService.GetUser();
            return await FoundAnyAuthor(user);
        }

        private async Task<bool> FoundAnyAuthor(User user) => await _appDbContext.Authors.AnyAsync(a => a.UserId == user.Id);
    }
}