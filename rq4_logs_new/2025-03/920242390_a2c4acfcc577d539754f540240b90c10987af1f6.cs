using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using API.Entities;
using Microsoft.EntityFrameworkCore;

namespace API.Data;

public class Seed
{
    public static async Task SeedUser(DataContext db)
    {
        if (await db.Users.AnyAsync())
        {
            return;
        }
        
        string userData = await File.ReadAllTextAsync("Data/UserSeedData.json");
        
        JsonSerializerOptions options = 
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        
        List<AppUsers>? users = 
            JsonSerializer.Deserialize<List<AppUsers>>(userData, options);

        if (users is null)
        {
            return;
        }

        foreach (AppUsers user in users)
        {
            using HMACSHA512 hmac = new HMACSHA512();

            user.UserName = user.UserName.ToLower();
            user.PasswordHash = hmac.ComputeHash(Encoding.UTF8.GetBytes("Pa$$w0rd"));
            user.PasswordSalt = hmac.Key;
            db.Users.Add(user);
        }
        await db.SaveChangesAsync();
    }
}