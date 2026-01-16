namespace MiniTwitTests
{
    using MiniTwit;
    using Xunit;

    public class UtilityTest
    {
        [Fact]
        public void GetGravatar_InputIsNormal_ReturnCorrectString()
        {
            string sampleEmailAddress = "sample@email.com";
            int sampleSize = 48;

            string expectedUrl =
                "http://www.gravatar.com/avatar/ca0b21b7f26ec580e57360c906c9529c?d=identicon&s=48";
            string gravatarUrl = Utility.GetGravatar(sampleEmailAddress, sampleSize);

            Assert.True(expectedUrl == gravatarUrl);
        }
    }
}
