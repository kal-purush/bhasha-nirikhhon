using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;

namespace InstagramProject.Models
{
    public class AccountManager
    {
        private readonly InstagramContext _context;

        public AccountManager(InstagramContext context)
        {
            _context = context;
        }

        public string ValidateNotEmptyAndUnique(string inputPrompt, string emptyErrorMessage, string duplicateErrorMessage, Func<User, string> fieldSelector)
        {
            while (true)
            {
                Console.WriteLine(inputPrompt);
                string userInput = Console.ReadLine()!;

                if (string.IsNullOrEmpty(userInput))
                {
                    Console.WriteLine(emptyErrorMessage);
                }
                else
                {
                    bool exists = _context.Users
                                          .Select(fieldSelector)
                                          .ToList()
                                          .Contains(userInput);

                    if (exists)
                    {
                        Console.WriteLine(duplicateErrorMessage);
                    }
                    else
                    {
                        return userInput;
                    }
                }
            }
        }

        public bool ValidatePasswordStrength(string password)
        {
            List<string> errors = new List<string>();

            if (password.Length < 8)
            {
                errors.Add("Password must be at least 8 characters long.");
            }
            if (!password.Any(char.IsUpper))
            {
                errors.Add("Password must contain at least one uppercase letter.");
            }
            if (!password.Any(char.IsLower))
            {
                errors.Add("Password must contain at least one lowercase letter.");
            }
            if (!password.Any(char.IsDigit))
            {
                errors.Add("Password must contain at least one digit.");
            }

            if (errors.Count > 0)
            {
                foreach (var error in errors)
                {
                    Console.WriteLine(error);
                }
                return false;
            }

            return true;
        }

        public string ValidateNotEmpty(string inputPrompt, string emptyErrorMessage)
        {
            while (true)
            {
                Console.WriteLine(inputPrompt);
                string userInput = Console.ReadLine()!;

                if (string.IsNullOrEmpty(userInput))
                {
                    Console.WriteLine(emptyErrorMessage);
                }
                else
                {
                    return userInput;
                }
            }
        }

        public void CreateAccount()
        {
            string userName = ValidateNotEmptyAndUnique(
                "Enter a username:",
                "Username cannot be empty. Please try again.",
                "Username already exists. Please choose another one.",
                user => user.UserName!
            );

            Console.WriteLine($"Username '{userName}' accepted!");

            string email = ValidateNotEmptyAndUnique(
                "Enter a email:",
                "Email cannot be empty. Please try again.",
                "Email already exists. Please choose another one.",
                user => user.Email!
            );

            Console.WriteLine($"Email '{email}' accepted!");

            string password;
            while (true)
            {
                Console.Write("Enter a password: ");
                password = Console.ReadLine()!;

                if (!ValidatePasswordStrength(password))
                {
                    Console.WriteLine("Please enter a stronger password.");
                }
                else
                {
                    Console.WriteLine("Password is strong enough.");
                    break;
                }
            }

            Console.WriteLine("Password accepted!");

            User newUser = new User
            {
                UserName = userName,
                Email = email,
                Password = password
            };

            _context.Users.Add(newUser);
            _context.SaveChanges();

            Console.WriteLine("User account created successfully!");
        }
    }
}