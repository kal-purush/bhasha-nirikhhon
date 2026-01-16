using EasyMicroservices.Utilities.Text;
using Xunit;

namespace EasyMicroservices.Utilities.Tests.Text
{
    public class HashHelperTest
    {
        [Theory]
        [InlineData("Ali")]
        [InlineData("Mahdi")]
        public void TestHash(string input)
        {
            Assert.NotEmpty(HashHelper.GetSHA1Hash(input));
        }

        [Theory]
        [InlineData("Ali", "Reza")]
        [InlineData("Ali", "Mahdi")]
        public void TestHashs(params string[] inputs)
        {
            Assert.NotEmpty(HashHelper.GetSHA1Hash(inputs));
        }
    }
}