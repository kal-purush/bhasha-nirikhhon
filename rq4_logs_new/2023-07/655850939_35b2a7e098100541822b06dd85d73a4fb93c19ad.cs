using Xunit;

namespace challenge12.Tests
{
    public class AnimalShelterTests
    {
        [Fact]
        public void TestEnqueueAndDequeue()
        {
            // Arrange
            var shelter = new AnimalShelter();
            var dog1 = new Animal { Species = "dog", Name = "Dog1" };
            var cat1 = new Animal { Species = "cat", Name = "Cat1" };

            // Act
            shelter.Enqueue(dog1);
            shelter.Enqueue(cat1);
            var dequeuedDog = shelter.Dequeue("dog");
            var dequeuedCat = shelter.Dequeue("cat");

            // Assert
            Assert.Equal(dog1, dequeuedDog);
            Assert.Equal(cat1, dequeuedCat);
        }

        [Fact]
        public void TestDequeueWithInvalidSpecies()
        {
            // Arrange
            var shelter = new AnimalShelter();
            var dog1 = new Animal { Species = "dog", Name = "Dog1" };

            // Act
            shelter.Enqueue(dog1);
            var dequeuedInvalid = shelter.Dequeue("invalid");

            // Assert
            Assert.Null(dequeuedInvalid);
        }

        [Fact]
        public void TestDequeueWhenNoAnimalsAvailable()
        {
            // Arrange
            var shelter = new AnimalShelter();

            // Act
            var dequeuedDog = shelter.Dequeue("dog");
            var dequeuedCat = shelter.Dequeue("cat");

            // Assert
            Assert.Null(dequeuedDog);
            Assert.Null(dequeuedCat);
        }
    }
}