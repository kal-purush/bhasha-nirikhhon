using WebApi.Attributes;

namespace WebApi.Tests.Attributes;

[TestFixture]
public class DefineConstantsAttributeTests
{
    [Test]
    public void AttributeInstantiation_WithConstantsString_IsNotNull()
    {
        // Arrange
        const string constantsString = "\"CONST1;CONST2;CONST3\"";

        // Act
        var attribute = new DefineConstantsAttribute(constantsString);

        // Assert
        Assert.That(attribute, Is.Not.Null);
    }

    [Test]
    public void ConstantsProperty_WithValidConstantsString_ParsesCorrectly()
    {
        // Arrange
        const string constantsString = "\"CONST1;CONST2;CONST3\"";
        var expectedConstants = new[]
        {
            "CONST1", "CONST2", "CONST3"
        };

        var attribute = new DefineConstantsAttribute(constantsString);

        // Act
        var actualConstants = attribute.Constants;

        // Assert
        Assert.That(expectedConstants, Is.EqualTo(actualConstants));
    }
}