using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace DemoStoreSpecflowProject.Support
{
    public static class RandomSupportFunctions
    {
        private static Random random = new Random();

        public static string GeneratePassword(int length)
        {
            const string uppercaseLetters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            const string lowercaseLetters = "abcdefghijklmnopqrstuvwxyz";
            const string numbers = "0123456789";

            var password = new StringBuilder();

            // Ensure at least one uppercase letter
            password.Append(uppercaseLetters[random.Next(uppercaseLetters.Length)]);

            // Ensure at least one lowercase letter
            password.Append(lowercaseLetters[random.Next(lowercaseLetters.Length)]);

            // Ensure at least one number
            password.Append(numbers[random.Next(numbers.Length)]);

            // Generate remaining characters
            for (int i = 3; i < length; i++)
            {
                string characters = uppercaseLetters + lowercaseLetters + numbers;
                password.Append(characters[random.Next(characters.Length)]);
            }

            // Shuffle the generated password
            var shuffledPassword = new string(password.ToString().ToCharArray().OrderBy(_ => random.Next()).ToArray());

            return shuffledPassword;
        }
    }
}