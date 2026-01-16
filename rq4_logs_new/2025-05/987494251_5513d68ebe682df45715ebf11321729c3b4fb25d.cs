using Microsoft.Extensions.Configuration;
using Xunit;

public class EncryptionHelperTests
{
    [Fact]
    public void Encrypt_And_Decrypt_ReturnsOriginalText()
    {
        // Arrange
        var key = Convert.ToBase64String(new byte[32]); // 256-bit key
        var iv = Convert.ToBase64String(new byte[16]);  // 128-bit IV

        var inMemorySettings = new Dictionary<string, string> {
            {"Encryption:Key", key},
            {"Encryption:IV", iv}
        };
        /*         Console.WriteLine($"Key: {key}");
                Console.WriteLine($"IV: {iv}"); */
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(inMemorySettings)
            .Build();

        SafeVault.EncryptionHelper.Initialize(configuration);

        var original = "Sensitive data!";

        // Act
        var encrypted = SafeVault.EncryptionHelper.Encrypt(original);
        var decrypted = SafeVault.EncryptionHelper.Decrypt(encrypted);
        Console.WriteLine($"Original: {original}");
        Console.WriteLine($"Encrypted: {encrypted}");
        // Assert
        Assert.Equal(original, decrypted);
    }
}