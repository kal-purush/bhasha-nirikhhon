using Hippologamus.Server.Factorys;
using System.Linq;
using Xunit;

namespace Hippologamus.Server.Test.Factory
{
    public class PerfLogsOrderByListFactoryTest
    {
        [Fact]
        public void Create_Count()
        {
            //arrange
            //act
            var value = PerfLogsOrderByListFactory.Create();
            //assert
            Assert.Equal(3, value.Count);
        }

        [Theory]
        [InlineData("ElapsedMilliseconds", "Elapsed Time")]
        [InlineData("MachineName", "Machine Name")]
        [InlineData("TimeStamp", "Time Stamp")]
        public void Create_Contains(string key, string expectedValue)
        {
            //arrange
            //act
            var value = PerfLogsOrderByListFactory.Create();
            //assert
            var item = value.First(x => x.Key == key);
            Assert.Equal(key, item.Key);
            Assert.Equal(expectedValue, item.Value);
        }
    }
}