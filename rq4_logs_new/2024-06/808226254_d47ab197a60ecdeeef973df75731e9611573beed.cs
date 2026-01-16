using ActivityFinder.Server.Database;

namespace ActivityFinder.Server.Models
{
    public interface IUserRepository
    {
        ApplicationUser GetByName(string userName);
    }

    public class UserRepository : GenericRepository<ApplicationUser>, IUserRepository
    {
        public UserRepository(AppDbContext context) : base(context)
        {                
        }

        public ApplicationUser GetByName(string userName)
        {
            return _context.Set<ApplicationUser>().SingleOrDefault(x => x.UserName == userName);
        }
    }
}