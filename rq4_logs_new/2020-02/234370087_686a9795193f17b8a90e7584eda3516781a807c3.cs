using Microsoft.EntityFrameworkCore;
using System;
using System.Linq;
using System.Threading.Tasks;
using TinyClothes.Data;
using TinyClothes.Models;

namespace TinyClothes
{
    public static class AccountDB
    {
        public static async Task<bool> IsUsernameTaken(string username, StoreContext context)
        {
            return await (from a in context.Account where username == a.Username select a).AnyAsync();
        }

        public static async Task<Account> Register(Account acc, StoreContext context)
        {
            await context.Account.AddAsync(acc);
            await context.SaveChangesAsync();

            return acc;
        }
    }
}