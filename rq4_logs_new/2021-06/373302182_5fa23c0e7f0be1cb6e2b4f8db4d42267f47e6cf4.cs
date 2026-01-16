using System;
using System.Collections.Generic;
using System.Text;
using NeutrackAPI.Models;

namespace NeutrackAPI.tests.Fixtures
{
    public static class GenerateUser
    {
        private static readonly Random _random = new Random();

        public static User GeneratData()
        {
            return new User
            {
                Id = 1,
                FirstName = RandomString(10, true),
                LastName = RandomString(10, true),
                Email = RandomString(10, true),
                Password = RandomString(10, true),
                Gender = RandomString(10, true),
                DateOfBirth = DateTime.Now
            };
        }

        public static List<User> GenerateListData(int quantity)
        {
            var users = new List<User>();
            for (int i = 1; i <= quantity; i++)
            {
                var user = GeneratData();
                user.Id = i;
                users.Add(user);
            }
            return users;
        }

        // Generates a random string with a given size.    
        public static string RandomString(int size, bool lowerCase = false)
        {
            var builder = new StringBuilder(size);

            // Unicode/ASCII Letters are divided into two blocks
            // (Letters 65–90 / 97–122):
            // The first group containing the uppercase letters and
            // the second group containing the lowercase.  

            // char is a single Unicode character  
            char offset = lowerCase ? 'a' : 'A';
            const int lettersOffset = 26; // A...Z or a..z: length=26  

            for (var i = 0; i < size; i++)
            {
                var @char = (char)_random.Next(offset, offset + lettersOffset);
                builder.Append(@char);
            }

            return lowerCase ? builder.ToString().ToLower() : builder.ToString();
        }
    }
}