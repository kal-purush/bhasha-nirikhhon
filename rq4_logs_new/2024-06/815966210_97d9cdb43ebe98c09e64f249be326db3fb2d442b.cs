using BCrypt.Net;

namespace Library
{
    public class PasswordHasher
    {
        // Method untuk melakukan hashing password
        public string HashPassword(string password)
        {
            // Work factor adalah parameter yang menentukan kompleksitas hashing
            // Nilai default biasanya 10, tetapi bisa diubah untuk menambah keamanan
            int workFactor = 12;
            return BCrypt.Net.BCrypt.HashPassword(password, workFactor);
        }

        // Method untuk memverifikasi password dengan hash yang tersimpan
        public bool VerifyPassword(string password, string hashedPassword)
        {
            return BCrypt.Net.BCrypt.Verify(password, hashedPassword);
        }
    }
}