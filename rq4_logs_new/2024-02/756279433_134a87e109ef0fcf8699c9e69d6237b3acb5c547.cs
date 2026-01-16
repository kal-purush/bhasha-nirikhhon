using AskHand.Domain.Climbs;

namespace AskHand.Test.Climbs;

public class ClimbIdTest
{
    [Fact]
    public void CreateUnique_ShouldReturnClimbId_WhenCalled()
    {
        // Act
        var climbId = ClimbId.CreateUnique();

        // Assert
        Assert.NotNull(climbId);
        Assert.IsType<ClimbId>(climbId);
    }

    [Fact]
    public void GetEqualityComponents_ShouldReturnEqual_WhenSameClimbIdProvided()
    {
        // Arrange
        var climbId1 = ClimbId.CreateUnique();

        // Act
        var result = climbId1.GetEqualityComponents().SequenceEqual(climbId1.GetEqualityComponents());

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void GetEqualityComponents_ShouldReturnNotEqual_WhenDifferentClimbIdProvided()
    {
        // Arrange
        var climbId1 = ClimbId.CreateUnique();
        var climbId2 = ClimbId.CreateUnique();

        // Act
        var result = climbId1.GetEqualityComponents().SequenceEqual(climbId2.GetEqualityComponents());

        // Assert
        Assert.False(result);
    }
}