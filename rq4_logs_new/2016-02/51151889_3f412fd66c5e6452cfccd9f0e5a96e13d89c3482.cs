namespace BoardGames.Services.Data
{
    using BoardGames.Services.Data.Contracts;
    using System.Linq;
    using BoardGames.Data.Models;
    using Microsoft.AspNet.Identity.EntityFramework;
    using Microsoft.AspNet.Identity;
    using System.Data.Entity;
    using Common;
    using Microsoft.AspNet.Identity.Owin;
    using Microsoft.Owin.Security.DataProtection;

    public class UsersService : IUsersService
    {
        private readonly DbContext context;
        private readonly UserManager<User> userManager;

        public UsersService(DbContext context)
        {
            this.context = context;
            var userStore = new UserStore<User>(context);
            userManager = new UserManager<User>(userStore);

            var provider = new DpapiDataProtectionProvider("BoardGames");

            userManager.UserTokenProvider = new DataProtectorTokenProvider<User>(
                provider.Create("UserToken"));
        }

        public void Create(string email, string username, string password, bool isAdmin)
        {
            var user = new User { UserName = username, Email = email };
            userManager.Create(user, password);

            if (isAdmin)
            {
                userManager.AddToRole(user.Id, GlobalConstants.AdministratorRoleName);
            }
        }

        public void Delete(string id)
        {
            var userToBeDeleted = userManager.FindById(id);
            userManager.Delete(userToBeDeleted);
        }

        public void Edit(string id, string email, string username, string password, bool isAdmin)
        {
            var userToBeEdited = userManager.FindById(id);

            userToBeEdited.Email = email;
            userToBeEdited.UserName = username;

            var token = userManager.GeneratePasswordResetToken(id);
            userManager.ResetPassword(id, token, password);

            if (isAdmin && userToBeEdited.Roles.Count == 0)
            {
                userManager.AddToRole(userToBeEdited.Id, GlobalConstants.AdministratorRoleName);
            }
            else if (!isAdmin && userToBeEdited.Roles.Count >= 1)
            {
                userManager.RemoveFromRole(userToBeEdited.Id, GlobalConstants.AdministratorRoleName);
            }

            userManager.Update(userToBeEdited);
        }

        public IQueryable<User> GetAll()
        {
            return userManager.Users;
        }
    }
}