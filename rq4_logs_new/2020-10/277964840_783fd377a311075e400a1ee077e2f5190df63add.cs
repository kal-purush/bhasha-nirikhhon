using F1Telemetry.WPF.Converter;
using FluentAssertions;
using System.Globalization;
using Xunit;

namespace F1Telemetry.Test.ConverterTests
{
    public class ToDeltaTimeStringTest
    {
        [Theory]
        [InlineData(-1, "-01.000")]
        [InlineData(-3.5, "-03.500")]
        [InlineData(1, "+01.000")]
        [InlineData(2, "+02.000")]
        [InlineData(3, "+03.000")]
        [InlineData(3.5, "+03.500")]
        [InlineData(10.5, "+10.500")]
        [InlineData(60, "+01:00.000")]
        [InlineData(70, "+01:10.000")]
        [InlineData(155.364, "+02:35.364")]
        [InlineData(112.12, "+01:52.120")]
        public void Convert_Should_Return_To_String_DeltaTime_Format_From_Seconds(float seconds, string expected)
        {
            // Arrange
            var converter = new ToDeltaTimeString();

            // Act
            var actual = converter.Convert(seconds, typeof(ToLapTimeStringTest), "", CultureInfo.InvariantCulture);

            // Assert
            actual.Should().Be(expected);
        }
    }
}