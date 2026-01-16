using ClassLibrary;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace Testing5
{
    [TestClass]
    public class tstOrder
    {
        [TestMethod]
        public void TestMethod1()
        {
                //create an instance of the class we want to create 
                clsOrder AnOrder = new clsOrder();
                //test to see that it exits
                Assert.IsNotNull(AnOrder);
        }

        [TestMethod]
        public void OrderIdOK()
        {
            // Create an instance of the class we want to create
            clsOrder AnOrder = new clsOrder();
            // Create some test data to assign to the property
            int testData = 1; // Change this to whatever test data you want
            // Assign the data to the property
            AnOrder.OrderId = testData;
            // Test to see that the two values are the same
            Assert.AreEqual(AnOrder.OrderId, testData);
        }

        [TestMethod]
        public void CustomerIdOK()
        {
            // Create an instance of the class we want to create
            clsOrder AnOrder = new clsOrder();
            // Create some test data to assign to the property
            int testData = 1; // Change this to whatever test data you want
            // Assign the data to the property
            AnOrder.CustomerId = testData;
            // Test to see that the two values are the same
            Assert.AreEqual(AnOrder.CustomerId, testData);
        }

        [TestMethod]
        public void TotalCostOK()
        {
            // Create an instance of the class we want to create
            clsOrder AnOrder = new clsOrder();
            // Create some test data to assign to the property
            double testData = 1.99; // Change this to whatever test data you want
            // Assign the data to the property
            AnOrder.TotalCost = testData;
            // Test to see that the two values are the same
            Assert.AreEqual(AnOrder.TotalCost, testData);
        }

        [TestMethod]
        public void OrderNotesOK()
        {
            // Create an instance of the class we want to create
            clsOrder AnOrder = new clsOrder();
            // Create some test data to assign to the property
            String testData = "Faulty"; // Change this to whatever test data you want
            // Assign the data to the property
            AnOrder.OrderNotes = testData;
            // Test to see that the two values are the same
            Assert.AreEqual(AnOrder.OrderNotes, testData);
        }
        [TestMethod]
        public void AddressOK()
        {
            // Create an instance of the class we want to create
            clsOrder AnOrder = new clsOrder();
            // Create some test data to assign to the property
            String testData = "1 Leiecter Road"; // Change this to whatever test data you want
            // Assign the data to the property
            AnOrder.Address = testData;
            // Test to see that the two values are the same
            Assert.AreEqual(AnOrder.Address, testData);
        }

        [TestMethod]
        public void DeliveryDateOK()
        {
            //create an instance of the class we wanta to create
            clsOrder AnOrder = new clsOrder();
            //create some test data to assign to the property
            DateTime TestData = DateTime.Now.Date;
            //assign the data to the property
            AnOrder.DeliveryDate = TestData;
            //Test to see that two values are the same 
            Assert.AreEqual(AnOrder.DeliveryDate, TestData);
        }
    }
}