using ClassLibrary;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace Testing2
{
    [TestClass]
    public class tstCustomer
    {
        [TestMethod]
        public void TestMethod1()
        {
            //create an instance of the class we want to create
            clsCustomer aCustomer = new clsCustomer();
            //test to see that it excists
            Assert.IsNotNull(aCustomer);
        }

        [TestMethod]
        public void CustomerIDOK()
        {
            //Create an instance of the class we want to create 
            clsCustomer aCustomer = new clsCustomer();
            //Create some test data to assign to the property
            int testData = 1;
            //Assign the data to the property
            aCustomer.CustomerID = testData;
            //test to see that the two values are the same
            Assert.AreEqual(aCustomer.CustomerID, testData);
        }
        [TestMethod]
        public void CustomernameIDOK()
        {
            //Create an instance of the class we want to create 
            clsCustomer aCustomer = new clsCustomer();
            //Create some test data to assingn to the property
            string testData = "adu rosa";
            //Assign the daata to the property
            aCustomer.CustomerName = testData;
            //test to see that the two values are the same
            Assert.AreEqual(aCustomer.CustomerName, testData);
        }
        [TestMethod]
        public void EmailAddressIDOK()
        {
            //Create an instance of the class we want to create 
            clsCustomer aCustomer = new clsCustomer();
            //Create some test data to assingn to the property
            string testData = "@";
            //Assign the daata to the property
            aCustomer.EmailAddress = testData;
            //test to see that the two values are the same
            Assert.AreEqual(aCustomer.EmailAddress, testData);
        }

        [TestMethod]
        public void PhonenumberIDOK()
        {
            //Create an instance of the class we want to create 
            clsCustomer aCustomer = new clsCustomer();
            //Create some test data to assingn to the property
            int testData = 1;
            //Assign the daata to the property
            aCustomer.phoneNumber = testData;
            //test to see that the two values are the same
            Assert.AreEqual(aCustomer.phoneNumber, testData);
        }
        [TestMethod]
        public void registrationIDOK()
        {
            //Create an instance of the class we want to create 
            clsCustomer aCustomer = new clsCustomer();
            //Create some test data to assign to the property
            DateTime testData = DateTime.Now;
            //Assign the data to the property
            aCustomer.registration = testData;
            //test to see that the two values are the same
            Assert.AreEqual(aCustomer.registration, testData);
        }
        [TestMethod]
        public void DateOfBirthPropertyOk()
        {
            // Create an instance of the class we want to test
            clsCustomer aCustomer = new clsCustomer();

            // Create some test data to assign to the property
            DateTime testData = DateTime.Now;

            // Assign the data to the property
            aCustomer.DateOfBirth = testData;

            // Test to see if the two values are the same
            Assert.AreEqual(aCustomer.DateOfBirth, testData);
        }
        [TestMethod]
        public void GenderPropertyOk()
        {
            // Create an instance of the class we want to test
            clsCustomer aCustomer = new clsCustomer();

            // Create some test data to assign to the property
            string testData = "Male"; // For example, a gender

            // Assign the data to the property
            aCustomer.Gender = testData;

            // Test to see if the two values are the same
            Assert.AreEqual(aCustomer.Gender, testData);
        }
        [TestMethod]
        public void InteractionIdPropertyOk()
        {
            // Create an instance of the class we want to test
            clsCustomer aCustomer = new clsCustomer();

            // Create some test data to assign to the property
            int testData = 123; // For example, an interaction ID

            // Assign the data to the property
            aCustomer.InteractionId = testData;

            // Test to see if the two values are the same
            Assert.AreEqual(aCustomer.InteractionId, testData);

        }
        [TestMethod]
        public void TimestampPropertyOk()
        {
            // Create an instance of the class we want to test
            clsCustomerInteraction interaction = new clsCustomerInteraction();

            // Get the current date and time
            DateTime currentTime = DateTime.Now;

            // Assign the current date and time to the Timestamp property
            interaction.Timestamp = currentTime;

            // Test to see if the assigned timestamp is within an acceptable range (e.g., within a few seconds of the current time)
            Assert.IsTrue((currentTime - interaction.Timestamp).TotalSeconds < 5); // Adjust the range as needed
        }
        [TestMethod]
        public void ActivityTypePropertyOk()
        {
            // Create an instance of the class we want to test
            clsCustomerInteraction interaction = new clsCustomerInteraction();

            // Create some test data for the activity type
            string testActivityType = "Login"; // Example activity type

            // Assign the test activity type to the ActivityType property
            interaction.ActivityType = testActivityType;

            // Test to see if the assigned activity type matches the test data
            Assert.AreEqual(interaction.ActivityType, testActivityType);
        }
        [TestMethod]
        public void DetailsPropertyOk()
        {
            // Create an instance of the class we want to test
            clsCustomerInteraction interaction = new clsCustomerInteraction();

            // Create some test data for the details
            string testDetails = "User logged in"; // Example details

            // Assign the test details to the Details property
            interaction.Details = testDetails;

            // Test to see if the assigned details match the test data
            Assert.AreEqual(interaction.Details, testDetails);
        }
        [TestMethod]
        public void SubscriptionIdPropertyOk()
        {
            // Create an instance of the class we want to test
            SubscriptionIdProperty subscription = new SubscriptionIdProperty();
            // Create some test data to assign to the property
            int testSubscriptionId = 123; // Example subscription ID

            // Assign the test data to the SubscriptionId property
            subscription.SubscriptionId = testSubscriptionId;

            // Test to see if the assigned subscription ID matches the test data
            Assert.AreEqual(subscription.SubscriptionId, testSubscriptionId);
        }
    }
}


