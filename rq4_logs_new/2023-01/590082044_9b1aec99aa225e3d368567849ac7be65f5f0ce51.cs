using System.Buffers.Text;
using System.Security.Cryptography;
using System.Text;
using CraftHouse.Web.Entities;
using Konscious.Security.Cryptography;

namespace CraftHouse.Web.Services;

public interface IAuthService
{
    void RegisterUser(User user, string password);
    bool VerifyUserPassword(User user, string password);
}

class AuthService : IAuthService
{
    public void RegisterUser(User user, string password)
    {
        //TODO: validate user
        
        var salt = CreateSalt();
        var hash = HashPassword(password, salt);

        var saltB64 = Convert.ToBase64String(salt);
        var hashB64 = Convert.ToBase64String(hash);

        user.PasswordHash = hashB64;
        user.PasswordSalt = saltB64;
    }

    public bool VerifyUserPassword(User user, string password)
    {
        var salt = Convert.FromBase64String(user.PasswordSalt);
        var hash = Convert.FromBase64String(user.PasswordHash);

        return VerifyHash(password, salt, hash);
    }


    private byte[] CreateSalt()
    {
        var buff = RandomNumberGenerator.GetBytes(16);
        return buff;
    }

    private byte[] HashPassword(string password, byte[] salt)
    {
        var argon2 = new Argon2id(Encoding.UTF8.GetBytes(password));

        argon2.Salt = salt;
        argon2.DegreeOfParallelism = 8;
        argon2.Iterations = 4;
        argon2.MemorySize = 1024 * 256;

        return argon2.GetBytes(16);
    }

    private bool VerifyHash(string password, byte[] salt, byte[] hash)
    {
        var newHash = HashPassword(password, salt);
        return hash.SequenceEqual(newHash);
    }
}