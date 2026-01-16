using Xunit;

namespace Common.Tests.UtilsClass
{
    public class FloatOperations
    {
        [Fact]
        public void GetFixedAsString()
        {
            var res1 = 0.001m.GetFixedAsString(5);
            var res2 = 15.123456m.GetFixedAsString(2);
            var res3 = 2m.GetFixedAsString(2);
            var res4 = 2m.GetFixedAsString(0);

            Assert.Equal("0.00100", res1);
            Assert.Equal("15.12", res2);
            Assert.Equal("2.00", res3);
            Assert.Equal("2", res4);
        }
    }
}