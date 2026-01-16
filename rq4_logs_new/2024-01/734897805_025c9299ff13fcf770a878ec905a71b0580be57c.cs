using System.Text.RegularExpressions;

namespace Exercise25
{
    static class Program
    {
        static void Main()
        {
            Console.Write("Enter the password you wish to test: ");
            string? password = Console.ReadLine();

            string complexity = CheckPasswordComplexity(password);
            Console.WriteLine($"The password '{password}' is a {complexity} password.");
        }

        static string CheckPasswordComplexity(string? password)
        {
            if (IsNumeric(password) && password!.Length < 8)
            {
                return "very weak";
            }

            if (IsAlpha(password) && password!.Length < 8)
            {
                return "weak";
            }

            if (Regex.IsMatch(password!, @"^(?=.*[A-Za-z])(?=.*\d)(?=.*[^A-Za-z0-9\s]).+$"))
            {
                return "strong";
            }
            
            if (Regex.IsMatch(password!, @"^(?=.*[A-Za-z])(?=.*\d)(?=.*[^A-Za-z0-9\s]).+$") && password!.Length >= 8)
            {
                return "very strong";
            }

            return "unknown"; 
        }

        static bool IsNumeric(string? value)
        {
            return int.TryParse(value, out _);
        }

        static bool IsAlpha(string? value)
        {
            return Regex.IsMatch(value!, @"^[A-Za-z]+$");
        }
    }
}