using TestBuildersPattern.ClassLibrary.Validators;
using UnitTests.TestBuilders;

namespace UnitTests.ValidatorsTests;
public sealed class ClientValidatorTests
{
    private readonly ClientValidator _clientValidator;

    public ClientValidatorTests()
    {
        _clientValidator = new ClientValidator();
    }

    [Fact]
    public async Task ValidateAsync_SuccessfulScenario_ReturnsTrue()
    {
        // A
        var validClient = ClientBuilder.NewObject().DomainBuild();

        // A
        var validationResult = await _clientValidator.ValidateAsync(validClient);

        // A
        Assert.True(validationResult.IsValid);
    }

    [Theory]
    [MemberData(nameof(InvalidNameParameters))]
    public async Task ValidateAsync_InvalidName_ReturnsFalse(string name)
    {
        // A
        var clientWithInvalidName = ClientBuilder.NewObject().WithName(name).DomainBuild();

        // A
        var validationResult = await _clientValidator.ValidateAsync(clientWithInvalidName);

        // A
        Assert.False(validationResult.IsValid);
    }

    public static TheoryData<string> InvalidNameParameters() =>
        new()
        {
            "",
            "a",
            new string('a', 152)
        };

    [Theory]
    [MemberData(nameof(InvalidDescriptionParameters))]
    public async Task ValidateAsync_InvalidDescription_ReturnsFalse(string description)
    {
        // A
        var clientWithInvalidDescription = ClientBuilder.NewObject().WithDescription(description).DomainBuild();

        // A
        var validationResult = await _clientValidator.ValidateAsync(clientWithInvalidDescription);

        // A
        Assert.False(validationResult.IsValid);
    }

    public static TheoryData<string> InvalidDescriptionParameters() =>
        new()
        {
            "",
            "a",
            new string('a', 1001)
        };

    [Theory]
    [MemberData(nameof(InvalidAuthorParameters))]
    public async Task ValidateAsync_InvalidAuthor_ReturnsFalse(string author)
    {
        // A
        var clientWithInvalidAuthor = ClientBuilder.NewObject().WithAuthor(author).DomainBuild();

        // A
        var validationResult = await _clientValidator.ValidateAsync(clientWithInvalidAuthor);

        // A
        Assert.False(validationResult.IsValid);
    }

    public static TheoryData<string> InvalidAuthorParameters() =>
        new()
        {
            "",
            "a",
            new string('a', 122)
        };

    [Theory]
    [InlineData(0)]
    [InlineData(-10)]
    [InlineData(-1)]
    public async Task ValidateAsync_InvalidAge_ReturnsFalse(int age)
    {
        // A
        var clientWithInvalidAge = ClientBuilder.NewObject().WithAge(age).DomainBuild();

        // A
        var validationResult = await _clientValidator.ValidateAsync(clientWithInvalidAge);

        // A
        Assert.False(validationResult.IsValid);
    }
}