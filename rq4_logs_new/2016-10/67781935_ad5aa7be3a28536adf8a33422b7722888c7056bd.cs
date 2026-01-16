using KataNetHack.Console.PlayerSubsystem;
using Xunit;

namespace KataNetHack.Tests
{
    public class PlayerTests
    {
        [Fact]
        public void Ctor_AtX2Y2_LocationX2Y2()
        {
            int x = 2;
            int y = 2;
            var player = new Player(x, y);
            Assert.Equal(2, player.Location.X);
            Assert.Equal(2, player.Location.Y);
        }

        [Fact]
        public void MovedNorth_WhenPlayerAtY2_YIs1()
        {
            int x = 2;
            int y = 2;
            var player = new Player(x, y);
            player.MovedNorth();
            Assert.Equal(1, player.Location.Y);
        }

        [Fact]
        public void MovedNorth_WhenPlayerAtX2_XIs2()
        {
            int x = 2;
            int y = 2;
            var player = new Player(x, y);
            player.MovedNorth();
            Assert.Equal(2, player.Location.X);
        }

        [Fact]
        public void MovedSouth_WhenPlayerAtY2_YIs3()
        {
            int x = 2;
            int y = 2;
            var player = new Player(x, y);
            player.MovedSouth();
            Assert.Equal(3, player.Location.Y);
        }

        [Fact]
        public void MovedSouth_WhenPlayerAtX2_XIs2()
        {
            int x = 2;
            int y = 2;
            var player = new Player(x, y);
            player.MovedSouth();
            Assert.Equal(2, player.Location.X);
        }

        [Fact]
        public void MovedWest_WhenPlayerAtX2_XIs1()
        {
            int x = 2;
            int y = 2;
            var player = new Player(x, y);
            player.MovedWest();
            Assert.Equal(1, player.Location.X);
        }

        [Fact]
        public void MovedWest_WhenPlayerAtY2_YIs2()
        {
            int x = 2;
            int y = 2;
            var player = new Player(x, y);
            player.MovedWest();
            Assert.Equal(2, player.Location.Y);
        }

        [Fact]
        public void MovedEast_WhenPlayerAtX2_XIs3()
        {
            int x = 2;
            int y = 2;
            var player = new Player(x, y);
            player.MovedEast();
            Assert.Equal(3, player.Location.X);
        }

        [Fact]
        public void MovedEast_WhenPlayerAtY2_YIs2()
        {
            int x = 2;
            int y = 2;
            var player = new Player(x, y);
            player.MovedEast();
            Assert.Equal(2, player.Location.Y);
        }
    }
}