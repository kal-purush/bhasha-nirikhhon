using AskHand.Domain.Friendships;

namespace AskHand.Test.Friendships;

public class FriendshipIdTest
{
    [Fact]
    public void CreateUnique_ShouldReturnFriendshipId_WhenCalled()
    {
        // Act
        var friendshipId = FriendshipId.CreateUnique();

        // Assert
        Assert.NotNull(friendshipId);
        Assert.IsType<FriendshipId>(friendshipId);
    }

    [Fact]
    public void GetEqualityComponents_ShouldReturnEqual_WhenSameFriendshipIdProvided()
    {
        // Arrange
        var friendshipId1 = FriendshipId.CreateUnique();

        // Act
        var result = friendshipId1.GetEqualityComponents().SequenceEqual(friendshipId1.GetEqualityComponents());

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void GetEqualityComponents_ShouldReturnNotEqual_WhenDifferentFriendshipIdProvided()
    {
        // Arrange
        var friendshipId1 = FriendshipId.CreateUnique();
        var friendshipId2 = FriendshipId.CreateUnique();

        // Act
        var result = friendshipId1.GetEqualityComponents().SequenceEqual(friendshipId2.GetEqualityComponents());

        // Assert
        Assert.False(result);
    }

}