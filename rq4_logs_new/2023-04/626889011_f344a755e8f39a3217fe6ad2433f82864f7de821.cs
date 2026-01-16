using System;

namespace Benraz.Infrastructure.Authorization.Passwords;

/// <summary>
/// Password generator.
/// </summary>
public class PasswordGenerator
{
    /// <summary>
    /// Generate valid password.
    /// </summary>
    /// <returns>Valid password.</returns>
    public static string GenerateValidPassword()
    {
        return GeneratePassword(true, true, true, true, false);
    }

    /// <summary>
    /// Generates a random password based on the rules passed in the parameters.
    /// </summary>
    /// <param name="includeLowercase">Boolean, if lowercase are required.</param>
    /// <param name="includeUppercase">Boolean, if uppercase are required.</param>
    /// <param name="includeNumeric">Boolean, if numerics are required.</param>
    /// <param name="includeSpecial">Boolean, if special characters are required.</param>
    /// <param name="includeSpaces">Boolean, if spaces are required.</param>
    /// <param name="lengthOfPassword">Length of password required, Should be between 8 and 128.</param>
    /// <returns></returns>
    private static string GeneratePassword(bool includeLowercase, bool includeUppercase, bool includeNumeric,
        bool includeSpecial, bool includeSpaces, int lengthOfPassword = 32)
    {
        const string LOWERCASE_CHARACTERS = "abcdefghijklmnopqrstuvwxyz";
        const string UPPERCASE_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        const string NUMERIC_CHARACTERS = "0123456789";
        const string SPECIAL_CHARACTERS = @"!#$%&*@\";
        const string SPACE_CHARACTER = " ";
        const int PASSWORD_LENGTH_MIN = 8;
        const int PASSWORD_LENGTH_MAX = 128;

        if (lengthOfPassword < PASSWORD_LENGTH_MIN || lengthOfPassword > PASSWORD_LENGTH_MAX)
        {
            throw new Exception("Password length must be between 8 and 128.");
        }

        var random = new Random();
        string characterSet = "";
        for (int i = 1; i < lengthOfPassword + 1; i++)
        {
            if ((includeLowercase) && (i % 1 == 0))
            {
                characterSet += LOWERCASE_CHARACTERS[random.Next(LOWERCASE_CHARACTERS.Length - 1)];
            }

            if ((includeUppercase) && (i % 2 == 0))
            {
                characterSet += UPPERCASE_CHARACTERS[random.Next(UPPERCASE_CHARACTERS.Length - 1)];
            }

            if ((includeNumeric) && (i % 3 == 0))
            {
                characterSet += NUMERIC_CHARACTERS[random.Next(NUMERIC_CHARACTERS.Length - 1)];
            }

            if ((includeSpecial) && (i % 4 == 0))
            {
                characterSet += SPECIAL_CHARACTERS[random.Next(SPECIAL_CHARACTERS.Length - 1)];
            }

            if ((includeSpaces) && (i % 5 == 0))
            {
                characterSet += SPACE_CHARACTER[random.Next(SPACE_CHARACTER.Length - 1)];
            }
        }

        return characterSet;
    }
}

