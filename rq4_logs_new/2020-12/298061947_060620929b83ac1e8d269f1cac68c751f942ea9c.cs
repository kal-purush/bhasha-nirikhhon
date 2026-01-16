using System;
using System.Collections.Generic;
using System.Linq;
using SmartSaver.EntityFrameworkCore.Models;
using SmartSaver.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace SmartSaver.EntityFrameworkCore.Repositories
{
    public class AccountRepo : IAccountRepo
    {
        private ApplicationDbContext _context;

        public AccountRepo(ApplicationDbContext context)
        {
            _context = context;
        }

        public Account GetAccountByUsername(string username)
        {
            return _context.Users
                .Include(u => u.Account)
                .First(u => u.Id.ToString().Equals(username))
                .Account;
        }

        public bool IsAccountValid(Account account)
        {
            return account.Goal > 0;
        }
    }
}