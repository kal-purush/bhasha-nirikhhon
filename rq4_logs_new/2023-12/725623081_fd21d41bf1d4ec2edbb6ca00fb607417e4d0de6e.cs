using System.ComponentModel.DataAnnotations;
using Shared;

namespace TestDomain;

[TestClass]
public class UnitTestUser
{
    [TestMethod]
    public void User_PropertiesShouldBeSetCorrectly()
    {
        // Arrange
        var user = new User
        {
            Id = 1,
            Mail = "test@example.com",
            Password = "securepassword",
            Username = "testuser"
        };

        // Act & Assert
        Assert.AreEqual(1, user.Id);
        Assert.AreEqual("test@example.com", user.Mail);
        Assert.AreEqual("securepassword", user.Password);
        Assert.AreEqual("testuser", user.Username);
    }
    
    [TestMethod]
    public void User_PropertiesShouldHaveAttributes()
    {
        // Arrange
        var user = new User();

        // Act
        var idProperty = typeof(User).GetProperty("Id");
        var keyAttribute = idProperty.GetCustomAttributes(typeof(KeyAttribute), true).FirstOrDefault() as KeyAttribute;
        var requiredAttribute = typeof(User).GetProperty("Mail").GetCustomAttributes(typeof(RequiredAttribute), true).FirstOrDefault() as RequiredAttribute;

        // Assert
        Assert.IsNotNull(keyAttribute);
        Assert.IsNotNull(requiredAttribute);
    }

    

}