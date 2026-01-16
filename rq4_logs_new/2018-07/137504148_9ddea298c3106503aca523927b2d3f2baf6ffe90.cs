using WordCount.Tests.XUnitHelpers;
using Xunit;
using WordCount.Extensions;

namespace WordCount.Tests.StringExtensionsTests
{
    public class FillRightWithPointsTests
    {
        [NamedFact]
        public void FillRightWithPointsTests_Text_bla_maxLength_10_Expect_Bla_and_7_Points()
        {
            string actual = "Bla".FillRightWithPoints(totalWidth: 10);
            const string expected = "Bla.......";

            Assert.Equal(
                expected: expected,
                actual: actual);
        }
    }
}