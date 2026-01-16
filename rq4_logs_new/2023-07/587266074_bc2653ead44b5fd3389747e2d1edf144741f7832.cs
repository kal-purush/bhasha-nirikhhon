using SelfService.Domain.Models;

namespace SelfService.Tests.Domain.Models;

public class TestKafkaTopicStatus
{
    [Theory]
    [MemberData(nameof(ValidInput))]
    public void try_parse_returns_expected_result_when_given_valid_input(string input)
    {
        var result = KafkaTopicStatus.TryParse(input, out _);
        Assert.True(result);
    }

    [Theory]
    [MemberData(nameof(InvalidInput))]
    public void try_parse_returns_expected_result_when_given_invalid_input(string? invalidInput)
    {
        var result = KafkaTopicStatus.TryParse(invalidInput, out var status);
        Assert.False(result);
        Assert.Null(status);
    }

    [Theory]
    [MemberData(nameof(InvalidInput))]
    public void parse_throws_expected_exception_on_invalid_input(string? invalidInput)
    {
        Assert.Throws<FormatException>(() => KafkaTopicStatus.Parse(invalidInput));
    }


    #region helpers

    public static IEnumerable<object[]> ValidInput
    {
        get
        {
            var validValues = new object[]
            {
                "Requested",
                "In Progress",
                "Provisioned"
            };

            return validValues.Select(x => new[] { x });
        }
    }

    public static IEnumerable<object[]> InvalidInput
    {
        get
        {
            var invalidValues = new object[]
            {
                "Request",
                "requested",
                "requesting",
                "InProgress",
                "In progress",
                "Progress",
                "provision",
                "provisioning",
                "Provisioning",
                "provisioned",
                null!
            };

            return invalidValues.Select(x => new[] { x });
        }
    }

    #endregion
}