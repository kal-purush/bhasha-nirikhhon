using System.Collections.Generic;
using System.Threading.Tasks;
using file.Core.Models;
using file.Core.Repositories;
using Microsoft.EntityFrameworkCore;

namespace file.Persistance.Repositories
{
    public class UserRepository : BaseRepository<User>, IUserRepository
    {
        public UserRepository(ApiDbContext context) : base(context)
        {
        }

        public async Task<IEnumerable<User>> GetAllUsersWithFiles()
        {
            return await _context.Users.Include(u => u.attachments).ToListAsync();
        }

        public async Task<User> GetUserWithFileById(int id)
        {
            return await _context.Users.Include(u => u.attachments).SingleOrDefaultAsync(u => u.id == id);
        }
    }
}