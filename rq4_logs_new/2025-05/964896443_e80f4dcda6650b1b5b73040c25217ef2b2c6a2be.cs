using AIPal.Commands;
using System.Threading.Tasks;
using Xunit;

namespace AIPal.Tests.Commands
{
    public class VolumeCommandTests
    {
        private readonly VolumeCommand _volumeCommand;

        public VolumeCommandTests()
        {
            _volumeCommand = new VolumeCommand();
        }

        [Theory]
        [InlineData("up 10")]
        [InlineData("up")]
        [InlineData("down 20")]
        [InlineData("down")]
        [InlineData("mute")]
        [InlineData("unmute")]
        public async Task Execute_WithValidParameters_ReturnsSuccess(string parameters)
        {
            // Act
            var result = await _volumeCommand.Execute(parameters);

            // Assert
            Assert.NotNull(result);
            Assert.Contains("volume", result.ToLower());
        }

        [Theory]
        [InlineData("")]
        [InlineData(null)]
        [InlineData("invalid")]
        [InlineData("up abc")]
        [InlineData("down -10")]
        public async Task Execute_WithInvalidParameters_ReturnsErrorMessage(string parameters)
        {
            // Act
            var result = await _volumeCommand.Execute(parameters);

            // Assert
            Assert.NotNull(result);
            Assert.Contains("usage", result.ToLower());
        }

        [Fact]
        public void CanHandle_WithVolumeKeywords_ReturnsTrue()
        {
            // Arrange
            string[] validInputs = new[]
            {
                "volume up",
                "volume down",
                "volume mute",
                "volume unmute",
                "turn up volume",
                "turn down volume",
                "increase volume",
                "decrease volume"
            };

            // Act & Assert
            foreach (var input in validInputs)
            {
                Assert.True(_volumeCommand.CanHandle(input));
            }
        }

        [Fact]
        public void CanHandle_WithNonVolumeKeywords_ReturnsFalse()
        {
            // Arrange
            string[] invalidInputs = new[]
            {
                "launch notepad",
                "open settings",
                "shutdown",
                "play music",
                "what time is it"
            };

            // Act & Assert
            foreach (var input in invalidInputs)
            {
                Assert.False(_volumeCommand.CanHandle(input));
            }
        }

        [Fact]
        public void GetHelpText_ReturnsNonEmptyString()
        {
            // Act
            var helpText = _volumeCommand.GetHelpText();

            // Assert
            Assert.NotNull(helpText);
            Assert.NotEmpty(helpText);
            Assert.Contains("volume", helpText.ToLower());
        }
    }
}