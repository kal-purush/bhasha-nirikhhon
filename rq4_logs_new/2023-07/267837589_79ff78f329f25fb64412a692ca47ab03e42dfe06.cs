namespace TextEncryption.Tests;

public class EncryptionTest
{
    private readonly StringEncryptor _stringEncryptor = new ();
    
    [Fact]
    public void SimpleEncryptionTest()
    {
        var secretKey = "SuperSecretKey";
        var plainString = "SuperSecretInfo";

        var encryptedString = _stringEncryptor.Encrypt(plainString, secretKey);
        Assert.NotEqual(encryptedString, plainString);
        
        var unencryptedString = _stringEncryptor.Decrypt(encryptedString, secretKey);
        Assert.Equal(unencryptedString, plainString);
    }
    
    [Fact]
    public void HardEncryptionTest()
    {
        var secretKey = "SuperSecretKey";
        var plainString = "SuperSecretInfo";

        var encryptedString = _stringEncryptor.Encrypt(plainString, secretKey);
        var encryptedString2 = _stringEncryptor.Encrypt(plainString, secretKey);
        Assert.NotEqual(encryptedString, plainString);
        Assert.NotEqual(encryptedString2, plainString);
        Assert.NotEqual(encryptedString, encryptedString2);
        
        var unencryptedString = _stringEncryptor.Decrypt(encryptedString, secretKey);
        var unencryptedString2 = _stringEncryptor.Decrypt(encryptedString2, secretKey);
        Assert.Equal(unencryptedString, plainString);
        Assert.Equal(unencryptedString2, plainString);
    }
}