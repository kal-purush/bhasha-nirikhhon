using KataNetHack.Console;
using Xunit;

namespace KataNetHack.Tests
{
    public class LocationTests
    {
        [Fact]
        public void Equals_WithEqualColumnAndRow_IsTrue()
        {
            var location1 = new Location(1,1);
            var location2 = new Location(1,1);
            Assert.Equal(location1,location2);
        }

        [Fact]
        public void Equals_WithDifferentColumnAndRow_IsTrue()
        {
            var location1 = new Location(1, 1);
            var location2 = new Location(2, 1);
            Assert.NotEqual(location1, location2);
        }
    }
}