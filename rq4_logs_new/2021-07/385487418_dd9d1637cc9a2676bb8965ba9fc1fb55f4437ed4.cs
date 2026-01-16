using NUnit.Framework;

namespace Hierarchy.Test
{
    public class Tests
    {
        private Animal[] _animals =
        {
            new Cat("Cat1", "Cat", 10, "Flat", "Persian"),
            new Tiger("Tiger1", "Tiger", 120, "Jungle"),
            new Zebra("Zebra1", "Zebra", 200, "Savanna"),
            new Mouse("Mouse1", "Mouse", 0.050, "Field")
        };
        private Vegetable _vegetable = new Vegetable(10);
        private Meat _meat = new Meat(2);

        [SetUp]
        public void Setup()
        {          
        }

        [Test]
        public void Animal_CheckGetMethods()
        {
            Assert.AreEqual(_animals[0].GetName(), "Cat1", "Name entry has not been saved correctly");
            Assert.AreEqual(_animals[0].GetType(), "Cat", "Type entry has not been saved correctly");
            Assert.AreEqual(_animals[0].GetWeight(), 12, 0.001, "Weight entry has not been saved correctly");
            Assert.AreEqual(_animals[0].GetLivingRegion(), "Flat", "Living region entry has not been saved correctly");
            Assert.AreEqual(new Cat("Cat2", "Cat", 10, "Flat", "Persian").GetBreed(), "Persian", "Breed entry has not been saved correctly");
        }

        [Test]
        public void Food_QuantityTest()
        {
            _vegetable = new Vegetable(10);
            _meat = new Meat(2);
            Assert.AreEqual(_vegetable.GetQuantity(), 10, 0.001, "Weight entry for Vegetables has not been saved correctly");
            Assert.AreEqual(_meat.GetQuantity(), 2, 0.001, "Weight entry for Meat has not been saved correctly");
        }

        [Test]
        public void Food_TypeTest()
        {
            Assert.AreEqual(_vegetable.FoodType(), "Vegetable", "Type entry for Vegetables has not been saved correctly");
            Assert.AreEqual(_meat.FoodType(), "Meat", "Type entry for Meat has not been saved correctly");
        }

        [Test]
        public void Food_EatenTest()
        {
            _vegetable = new Vegetable(10);
            _meat = new Meat(2);
            _vegetable.Eaten(1);
            _meat.Eaten(0.5);
            Assert.AreEqual(_vegetable.GetQuantity(), 9, 0.001, "Weight for Vegetables does not decrease accordingly after being eaten");
            Assert.AreEqual(_meat.GetQuantity(), 1.5, 0.001, "Weight for Meat does not decrease accordingly after being eaten");
        }

        [Test]
        public void Food_OverQuantityEatenTest()
        {
            _vegetable = new Vegetable(10);
            _meat = new Meat(2); 
            _vegetable.Eaten(20);
            _meat.Eaten(5);
            Assert.AreEqual(_vegetable.GetQuantity(), 0, 0.001, "Weight for Vegetables should be 0 if over eaten");
            Assert.AreEqual(_meat.GetQuantity(), 0, 0.001, "Weight for Meat should be 0 if over eaten");
        }

        [Test]
        public void Animal_Cat_EatTest()
        {
            _vegetable = new Vegetable(10);
            _meat = new Meat(2);
            _animals[0].Eat(_vegetable, 1);
            Assert.AreEqual(_animals[0].GetWeight(), 11, 0.001, "Cat eats vegetables an it's weight increases. Did not pass.");
            Assert.AreEqual(_vegetable.GetQuantity(), 9, 0.001, "Weight for Vegetables should decrease after being eaten by animal. Did not pass.");
            _animals[0].Eat(_meat, 1);
            Assert.AreEqual(_animals[0].GetWeight(), 12, 0.001, "Cat eats meat an it's weight increases. Did not pass.");
            Assert.AreEqual(_meat.GetQuantity(), 1, 0.001, "Weight for Meat should decrease after being eaten by animal. Did not pass.");
        }

        [Test]
        public void Animal_Tiger_EatTest()
        {
            _vegetable = new Vegetable(10);
            _meat = new Meat(2);
            _animals[1].Eat(_vegetable, 1);
            Assert.AreEqual(_animals[1].GetWeight(), 120, 0.001, "Tiger does not eat vegetables an it's weight does not increase. Did not pass.");
            Assert.AreEqual(_vegetable.GetQuantity(), 10, 0.001, "Weight for Vegetables should not decrease as Tiger does not eat Vegetables. Did not pass.");
            _animals[1].Eat(_meat, 1);
            Assert.AreEqual(_animals[1].GetWeight(), 121, 0.001, "Tiger eats meat an it's weight increases. Did not pass.");
            Assert.AreEqual(_meat.GetQuantity(), 1, 0.001, "Weight for Meat should decrease after being eaten by animal. Did not pass.");
        }

        public void Animal_Zebra_EatTest()
        {
            _vegetable = new Vegetable(10);
            _meat = new Meat(2);
            _animals[2].Eat(_vegetable, 1);
            Assert.AreEqual(_animals[2].GetWeight(), 201, 0.001, "Zebra eats vegetables and it's weight increases. Did not pass.");
            Assert.AreEqual(_vegetable.GetQuantity(), 9, 0.001, "Weight for Vegetables should decrease after being eaten by animal. Did not pass.");
            _animals[2].Eat(_meat, 1);
            Assert.AreEqual(_animals[2].GetWeight(), 201, 0.001, "Tiger eats meat and it's weight increases. Did not pass.");
            Assert.AreEqual(_meat.GetQuantity(), 2, 0.001, "Weight for Meat should not decrease as Zebra does not eat Meat. Did not pass.");
        }

        public void Animal_Mouse_EatTest()
        {
            _vegetable = new Vegetable(0.02);
            _meat = new Meat(0.02);
            _animals[3].Eat(_vegetable, 0.02);
            Assert.AreEqual(_animals[3].GetWeight(), 0.07, 0.001, "Mouse eats vegetables and it's weight increases. Did not pass.");
            Assert.AreEqual(_vegetable.GetQuantity(), 0 , 0.001, "Weight for Vegetables should decrease after being eaten by animal. Did not pass.");
            _animals[3].Eat(_meat, 0.02);
            Assert.AreEqual(_animals[3].GetWeight(), 0.07, 0.001, "Mouse eats meat and it's weight increases. Did not pass.");
            Assert.AreEqual(_meat.GetQuantity(), 0.02, 0.001, "Weight for Meat should not decrease as Mouse does not eat Meat. Did not pass.");
        }
    }
}