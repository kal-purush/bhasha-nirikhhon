using System.Text;

class Program
{
    static void Main()
    {
        Console.Write("What's the minimum length? ");
        int minLength = GetPositiveIntegerInput();
        
        Console.Write("How many special characters? ");
        int specialCharCount = GetNonNegativeIntegerInput();
        
        Console.Write("How many numbers? ");
        int numberCount = GetNonNegativeIntegerInput();

        string password = GeneratePassword(minLength, specialCharCount, numberCount);
        
        Console.WriteLine($"Your password is {password}. ");
        Console.ReadLine();
    }

    static int GetPositiveIntegerInput()
    {
        int input;
        while (!int.TryParse(Console.ReadLine(), out input) || input <= 0)
        {
            Console.WriteLine("Please enter a positive integer: ");
        }
        return input;
    }

    static int GetNonNegativeIntegerInput()
    {
        int input;
        while (!int.TryParse(Console.ReadLine(), out input) || input < 0)
        {
            Console.WriteLine("Please enter a non-negative integer: ");
        }
        return input;
    }

    static string GeneratePassword(int minLength, int specialCharCount, int numberCount)
    {
        string lowercaseChars = "abcdefghijklmnopqrstuvwxyz";
        string uppercaseChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        string specialChars = "!@#$%^&*()_+-=[]{}|;:`\",<>/?";
        string numberChars = "0123456789";

        Random random = new Random();
        StringBuilder password = new StringBuilder();

        for (int i = 0; i < minLength - specialCharCount - numberCount; i++)
        {
            password.Append(lowercaseChars[random.Next(lowercaseChars.Length)]);
        }

        for (int i = 0; i < specialCharCount; i++)
        {
            password.Append(specialChars[random.Next(specialChars.Length)]);
        }

        for (int i = 0; i < numberCount; i++)
        {
            password.Append(numberChars[random.Next(numberChars.Length)]);
        }

        string shuffledPassword = new string(password.ToString().OrderBy(c => random.Next()).ToArray() as char[]);

        return shuffledPassword;
    }
}