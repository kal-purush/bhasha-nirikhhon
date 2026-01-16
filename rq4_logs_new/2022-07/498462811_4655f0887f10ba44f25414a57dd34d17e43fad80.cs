using Michaelvsk.GameDb.Core.Errors;

namespace Michaelvsk.GameDb.Test.Core.Errors;
public class NotFoundTests
{
    [Fact]
    public void Set_Null_Message_ThrowsArgumentNullException()
    {
        string? message = null;

        Action actual = () => new NotFound(message);

        Assert.Throws<ArgumentNullException>(actual);
    }

    [Fact]
    public void Set_Empty_Message_ThrowsArgumentException()
    {
        string message = string.Empty;
        
        Action actual = () => new NotFound(message);

        Assert.Throws<ArgumentException>(actual);
    }
}