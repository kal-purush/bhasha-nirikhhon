using rb.dal.Data;
using rb.dal.Models;
using rb.dal.Repositories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace rb.bll
{
    public class UserService
    {
        public static bool RegisterUser(string Username, string Password, string Email)
        {
            using(ResumeBuilderContext context = new ResumeBuilderContext())
            {
                GenericRepository<User> genericRepository = new(context);

                List<User> users = genericRepository.GetAll().ToList();
                if (users.FirstOrDefault(u => u.Username == Username) != null)
                {
                    return false;
                }

                User user = new User()
                {
                    Username = Username,
                    Email = Email
                };

                user.Salt = GenerateSalt();
                string saltedPassword = Password + user.Salt;
                user.Password = HashPassword(saltedPassword);

                genericRepository.Add(user);
                context.SaveChanges();
                return true;
            }
        }
        
        public static string GenerateSalt()
        {
            Random rnd = new Random();

            byte[] rndBytes = new byte[16];
            rnd.NextBytes(rndBytes);

            string salt = Convert.ToHexString(rndBytes);

            return salt;
        }

        public static string HashPassword(string password)
        {
            SHA256 hash = SHA256.Create();

            byte[] passBytes = Encoding.Default.GetBytes(password);

            string hashedPass = Convert.ToHexString(hash.ComputeHash(passBytes));

            return hashedPass;
        }

    }
}