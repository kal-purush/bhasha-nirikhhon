using Xunit;

namespace BackEndFucntions.Tests.UtilsTests
{
    public class ValidateActivitiesInputTest
    {
        private static readonly List<dynamic> transportsCorrect = new()
            {
                new { TransportType = "Car", Miles = "10.5" },
            };
        private static readonly List<dynamic> foodsCorrect = new()
            {
                new { foodType = "Egg", Amount = "1" },
            };
        private static readonly List<dynamic> utilitiesCorrect = new()
            {
                new { utilities = "Car", Hours = "10" },
            };
        private static readonly List<dynamic> transportsWrong = new()
            {
                new { TransportType = "Car", Miles = "10t0.5" },
            };
        private static readonly List<dynamic> foodsWrong = new()
            {
                new { foodType = "Egg", Amount = "137y" },
            };
        private static readonly List<dynamic> utilitiesWrong= new()
            {
                new { utilities = "Car", Hours = "10;" },
            };
        [Theory]
        [InlineData("Wrong", "Correct", "Correct", false)]
        [InlineData("Correct", "Wrong", "Correct", false)]
        [InlineData("Correct", "Correct", "Wrong", false)]
        [InlineData("Correct", "Wrong", "Wrong", false)]
        [InlineData("Wrong", "Wrong", "Wrong", false)]
        public void IncorrectInput(string transportVersion, string foodVersion, string utilitiesVersion, bool EXPECTED_RESPONSE)
        {
            List<dynamic> transports;
            List<dynamic> foods;
            List<dynamic> utilities;
            transports = (transportVersion == "Correct") ? transportsCorrect : transportsWrong;
            foods = (foodVersion == "Correct") ? foodsCorrect : foodsWrong;
            utilities = (utilitiesVersion == "Correct") ? utilitiesCorrect : utilitiesWrong;

            bool validated = ValidateActivitiesInput.ValidateNumbers(transports, foods, utilities);
            Assert.Equal(EXPECTED_RESPONSE, validated);
        }
        [Fact]
        public void CorrectInput()
        {
            List<dynamic> transports = transportsCorrect;
            List<dynamic> foods = foodsCorrect;
            List<dynamic> utilities = utilitiesCorrect;
            const bool EXPECTED_RESULT = true;
            bool validated = ValidateActivitiesInput.ValidateNumbers(transports, foods, utilities);
            Assert.Equal(EXPECTED_RESULT, validated);
        }
    }
}