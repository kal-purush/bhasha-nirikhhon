using Xunit;

namespace CSharpTestDrivenDevelopment.Tests
{
    public class PasswordTest
    {
        [Fact]
        public void Validate_GivenLongerThan8Chars_ReturnsTrue()
        {
            var testTarget = new DefaultPasswordValidator();
            var password = MakeString(7)+"A";
            var validationResult = testTarget.Validate(password);

            Assert.True(validationResult);
        }

        [Fact]
        public void Validate_GivenLongerThan8Chars_ReturnsFalse()
        {
            var testTarget = new DefaultPasswordValidator();
            var password = MakeString(7);
            var validationResult = testTarget.Validate(password);

            Assert.False(validationResult);
        }

        [Fact]
        public void Validate_GivenNoUpperCase_ReturnsFalse()
        {
            var testTarget = new DefaultPasswordValidator();
            var password = "abc"; // all lowercase
            var validationResult = testTarget.Validate(password);

            Assert.False(validationResult);
        }

        [Fact]
        public void Validate_GivenOneUpperCase_ReturnsTrue()
        {
            var testTarget = new DefaultPasswordValidator();
            var password = MakeString(8)+"A"; // at least 1 Uppercase
            var validationResult = testTarget.Validate(password);

            Assert.True(validationResult);
        }

        private string MakeString(int length)
        {
            string result = "";
            for (int i = 1;i<= length; i++)
            {
                result += "a";
            }
            return result;
        }
    }
}