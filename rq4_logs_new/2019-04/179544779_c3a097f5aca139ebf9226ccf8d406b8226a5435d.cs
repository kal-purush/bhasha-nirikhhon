using System.Threading.Tasks;
using DatingApi.API.Models;

namespace DatingApi.API.Data
{
    public class AuthRepository : IAuthRepository
    {
        private readonly DataContext _context;
        public AuthRepository(DataContext context)
        {
            _context = context;
        }

        public Task<User> Login(string user, string password){
            throw new System.NotImplementedException();
        }

        public async Task<User> Register(User user, string password){
            byte[] passwordHash, passwordSalt;
            CreatePasswordHash(password, out passwordHash, out passwordSalt);

            user.PasswordHash = passwordHash;
            user.PasswordSalt = passwordSalt;

            await _context.Users.AddAsync(user);
            await _context.SaveChangesAsync();

            return user;
        }

        public void CreatePasswordHash(string password, out byte[] passwordHash, out byte[] passwordSalt){
            using(var hkey = new System.Security.Cryptography.HMACSHA512()){
                passwordSalt = hkey.Key;
                passwordHash = hkey.ComputeHash(System.Text.Encoding.UTF8.GetBytes(password));
            }

        }

        public Task<bool> UserExist(string username)
        {
            throw new System.NotImplementedException();
        }
    }
}