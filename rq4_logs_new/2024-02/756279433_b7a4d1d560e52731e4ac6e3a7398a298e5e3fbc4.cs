using AskHand.Domain.Users;

namespace AskHand.Test.Users;

public class UserIdTest
{
    [Fact]
    public void CreateUnique_ShouldReturnUserId_WhenCalled()
    {
        // Act
        var userId = UserId.CreateUnique();

        // Assert
        Assert.NotNull(userId);
        Assert.IsType<UserId>(userId);
    }

    [Fact]
    public void GetEqualityComponents_ShouldReturnEqual_WhenSameUserIdProvided()
    {
        // Arrange
        var userId1 = UserId.CreateUnique();

        // Act
        var result = userId1.GetEqualityComponents().SequenceEqual(userId1.GetEqualityComponents());

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void GetEqualityComponents_ShouldReturnNotEqual_WhenDifferentUserIdProvided()
    {
        // Arrange
        var userId1 = UserId.CreateUnique();
        var userId2 = UserId.CreateUnique();

        // Act
        var result = userId1.GetEqualityComponents().SequenceEqual(userId2.GetEqualityComponents());

        // Assert
        Assert.False(result);
    }
}