using API.DTOs;
using API.Entites;
using Microsoft.EntityFrameworkCore;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace API.Data
{
    public class Seed
    {

        public static async Task SeedUsers(DataContext context)
        {
            if (await context.User.AnyAsync()) return; // context.User.AnyAsync() checks if any users exist in the database.

            var userData = await File.ReadAllTextAsync("Data/UserSeedData.json");

            var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

            var users = JsonSerializer.Deserialize<List<AppUser>>(userData);

            foreach (var user in users)
            {

                var hmac = new HMACSHA512();

                user.UserName = user.UserName.ToLower();
                user.PasswordHash = hmac.ComputeHash(Encoding.UTF8.GetBytes("admin"));

                //Console.WriteLine($"PasswordHash (Base64): {Convert.ToBase64String(user.PasswordHash)}");
                //Console.WriteLine($"PasswordSalt (Base64): {Convert.ToBase64String(user.PasswordSalt)}");

                user.PasswordSalt = hmac.Key;

                context.User.Add(user); 
            }

            await context.SaveChangesAsync();
        }
    }
}