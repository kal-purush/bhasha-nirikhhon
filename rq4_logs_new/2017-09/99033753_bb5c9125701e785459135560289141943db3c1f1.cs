using Microsoft.VisualStudio.TestTools.UnitTesting;
using toofz.NecroDancer.Replays;

namespace toofz.NecroDancer.Tests.Replays
{
    class MoveTests
    {
        [TestClass]
        public class Constructor
        {
            [TestMethod]
            public void ReturnsInstance()
            {
                // Arrange -> Act
                var move = new Move();

                // Assert
                Assert.IsInstanceOfType(move, typeof(Move));
            }
        }

        [TestClass]
        public class BeatProperty
        {
            [TestMethod]
            public void GetSetBehavior()
            {
                // Arrange
                var move = new Move();
                var beat = 20;

                // Act
                move.Beat = beat;
                var beat2 = move.Beat;

                // Assert
                Assert.AreEqual(beat, beat2);
            }
        }

        [TestClass]
        public class IdProperty
        {
            [TestMethod]
            public void GetSetBehavior()
            {
                // Arrange
                var move = new Move();
                var id = 1;

                // Act
                move.Id = id;
                var id2 = move.Id;

                // Assert
                Assert.AreEqual(id, id2);
            }
        }
    }
}