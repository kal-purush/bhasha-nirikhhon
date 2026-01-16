using Xunit;
using data_structures_and_algorithms_.challenge11.pseudo_queue;

namespace TestProject1
{
    public class PseudoQueueTests
    {
        [Fact]
        public void Enqueue_ShouldAddElementToQueue()
        {
            // Arrange
            var pseudoQueue = new PseudoQueue<int>();

            // Act
            pseudoQueue.Enqueue(10);
            pseudoQueue.Enqueue(20);

            // Assert
            Assert.Equal(10, pseudoQueue.Dequeue());
            Assert.Equal(20, pseudoQueue.Dequeue());
        }

        [Fact]
        public void Dequeue_ShouldRemoveAndReturnElementFromQueue()
        {
            // Arrange
            var pseudoQueue = new PseudoQueue<int>();

            // Act
            pseudoQueue.Enqueue(10);
            pseudoQueue.Enqueue(20);

            // Assert
            Assert.Equal(10, pseudoQueue.Dequeue());
            Assert.Equal(20, pseudoQueue.Dequeue());
        }

        [Fact]
        public void Dequeue_ShouldReturnNull_WhenQueueIsEmpty()
        {
            // Arrange
            var pseudoQueue = new PseudoQueue<int>();

            // Act
            var result = pseudoQueue.Dequeue();

            // Assert
            Assert.Null(result);
        }
    }
}