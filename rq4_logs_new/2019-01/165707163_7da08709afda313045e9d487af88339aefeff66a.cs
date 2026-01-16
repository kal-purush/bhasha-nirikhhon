using System;
using Xunit;
using AnimalShelter.Classes;

namespace AnimalShelterTests
{
    public class EnqueueTests
    {
        [Fact]
        public void EnqueueIntoEmptyShelter()
        {
            Shelter shelter = new Shelter();
            Animal dog = new Animal("Dog");

            shelter.Enqueue(dog);

            Assert.True(shelter.Rear.Type == "Dog");
            Assert.True(shelter.Front.Type == "Dog");
        }
        [Fact]
        public void EnqueueIntoShelter()
        {
            Shelter shelter = new Shelter();
            Animal dog = new Animal("Dog");
            Animal cat = new Animal("Cat");
            Animal turtle = new Animal("Turtle");


            shelter.Enqueue(dog);
            shelter.Enqueue(cat);
            shelter.Enqueue(turtle);

            Assert.True(shelter.Rear.Type == "Cat");
            Assert.True(shelter.Front.Type == "Dog");
        }
    }
    public class DequeueTests
    {
        [Fact]
        public void DequeueOnEmptyShelter()
        {
            Shelter shelter = new Shelter();
            Animal dog = new Animal("Dog");

            Assert.Throws<NullReferenceException>(() => shelter.Dequeue(dog));
        }
        [Fact]
        public void DequeueOtherAnimalOnShelter()
        {
            Shelter shelter = new Shelter();
            Animal dog = new Animal("Dog");
            Animal cat = new Animal("Cat");
            Animal turtle = new Animal("Turtle");

            shelter.Enqueue(dog);
            shelter.Enqueue(dog);
            shelter.Enqueue(dog);
            shelter.Enqueue(cat);
            shelter.Enqueue(cat);
            shelter.Enqueue(dog);

            Assert.True(shelter.Dequeue(turtle).Type == dog.Type);
        }
        [Fact]
        public void DequeueCatAnimalOnShelterMix()
        {
            Shelter shelter = new Shelter();
            Animal dog = new Animal("Dog");
            Animal cat = new Animal("Cat");
            Animal turtle = new Animal("Turtle");

            Animal testCat = new Animal("Cat");

            shelter.Enqueue(dog);
            shelter.Enqueue(dog);
            shelter.Enqueue(dog);
            shelter.Enqueue(cat);
            shelter.Enqueue(dog);
            shelter.Enqueue(cat);


            Assert.True(shelter.Dequeue(cat).Type == cat.Type);
            Assert.Equal(testCat.Type, shelter.Rear.Type);
        }
    }
}