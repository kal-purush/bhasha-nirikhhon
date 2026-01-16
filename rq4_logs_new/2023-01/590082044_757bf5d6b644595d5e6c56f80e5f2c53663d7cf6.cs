using System.Security.Cryptography;
using System.Text;
using Konscious.Security.Cryptography;

namespace CraftHouse.Web.Helpers;

public static class HashingHelper
{
    
    public static byte[] CreateSalt()
    {
        var buff = RandomNumberGenerator.GetBytes(16);
        return buff;
    }

    public static byte[] HashPassword(string password, byte[] salt)
    {
        var argon2 = new Argon2id(Encoding.UTF8.GetBytes(password));

        argon2.Salt = salt;
        argon2.DegreeOfParallelism = 8;
        argon2.Iterations = 4;
        argon2.MemorySize = 1024 * 256;

        return argon2.GetBytes(16);
    }

    public static bool VerifyHash(string password, byte[] salt, byte[] hash)
    {
        var newHash = HashPassword(password, salt);
        return hash.SequenceEqual(newHash);
    }
}