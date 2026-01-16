using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Testing2
{
    // Define the clsCustomer class
    public class clsCustomer
    {
        
    }

    // Test class
    [TestClass]
    public class UnitTest1
    {
        // Test method
        [TestMethod]
        public void TestCustomer()
        {
            // Create an instance of the class we want to test
            clsCustomer aCustomer = new clsCustomer();
            // Test to see that it exists
            Assert.IsNotNull(aCustomer);
        }
    }
}