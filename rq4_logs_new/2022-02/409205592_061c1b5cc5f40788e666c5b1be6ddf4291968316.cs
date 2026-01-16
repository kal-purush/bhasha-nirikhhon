using Core.Features;
using Core.Helpers;
using DTO.Models;

namespace Core.Extensions
{
    /// <summary>
    /// Extensions methods for User.
    /// </summary>
    public static class UserExtensions
    {
        /// <summary>
        /// Checks if the password from user's account is equal with the one that was provided.
        /// </summary>
        /// <param name="user">User's account.</param>
        /// <param name="password">Password to check.</param>
        /// <param name="encryptionSettings">EncryptionSettings that provides the encryption KEY.</param>
        /// <returns></returns>
        public static bool CheckPassword(this User user, string password, IEncryptionSettings encryptionSettings)
        {
            if (password.Length == 0)
                return false;

            if (PasswordEncryption.Decrypt(user.Password, encryptionSettings) == password)
                return true;

            return false;
        }
    }
}