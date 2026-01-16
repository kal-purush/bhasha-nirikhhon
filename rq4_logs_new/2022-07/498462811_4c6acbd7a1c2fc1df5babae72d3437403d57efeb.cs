namespace GameDb.Models.Tests;

public class CoverTests
{
    [Fact]
    public void Assign_DefaultGuid_AsId_ThrowsArgumentException()
    {
        byte[] data = { 1 };
        Cover cover;

        Action actual = () => cover = new Cover(Guid.Empty, data);

        Assert.Throws<ArgumentException>(actual);
    }

    [Fact]
    public void Assign_Null_PngData_ThrowsArgumentNullException()
    {
        byte[]? data = null;
        Cover cover;

        #pragma warning disable CS8604 // Possible null reference argument.
        Action actual = () => cover = new Cover(data);
        #pragma warning restore CS8604 // Possible null reference argument.

        Assert.Throws<ArgumentNullException>(actual);
    }

    [Fact]
    public void Assign_EmptyByteArray_PngData_ThrowsArgumentOutOfRangeException()
    {
        byte[]? data = new byte[] { };
        Cover cover;

        Action actual = () => cover = new Cover(data);

        Assert.Throws<ArgumentOutOfRangeException>(actual);
    }    
}