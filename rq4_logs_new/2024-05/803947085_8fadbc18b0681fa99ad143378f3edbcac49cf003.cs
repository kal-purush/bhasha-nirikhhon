using Event_Planinng_System_DAL.Models;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure.Internal;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Threading.Tasks;

namespace Event_Planinng_System_DAL.Repos
{
    public class UserRepo
    {
        readonly UserManager<User> UserManager;
        readonly dbContext db;
        public UserRepo(UserManager<User> _user, dbContext db)
        {
            UserManager = _user;
            this.db = db;

        }

        public async Task<List<User>> GetAll()
        {
            return await db.Users.ToListAsync();
        }

        public async Task Edit(User model)
        {
            if (model == null)
            {
                throw new ArgumentNullException("model");
            }
            db.Entry(model).State = EntityState.Modified;
            await db.SaveChangesAsync();
        }


        public async Task Add (User model , string password)
        {
            if (model == null)
            {
                throw new ArgumentNullException("model");
            }
           await UserManager.CreateAsync(model , password);
        }

        public async Task<User?> FindById (int id)
        {
            return await UserManager.FindByIdAsync(id.ToString());
        }

        public async Task Delete(User model)
        {
            model.IsDeleted = true;
            await db.SaveChangesAsync();
        }

        public async Task<User?> FindByEmail(string email)
        {
            return await UserManager.FindByEmailAsync(email);
        }

        public async Task<List<string>?> GetAllRoles(User model)
        {
            return await UserManager.GetRolesAsync(model) as List<string>;
        }

        public async Task AddUserToRoles(User model)
        {
            await UserManager.AddToRolesAsync(model, new List<string> { "Planner" , "User" });
        }
    }
}