using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Lunabank.Data.Entities;

namespace Lunabank.Data.Repos
{
    public class UserRepo : IUserRepo
    {
        private readonly DataContext _context;

        public UserRepo(DataContext context)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }
        public bool EmailExist(string email)
        {
            return _context.Users.Any(c => c.Email == email);
        }
    }
}