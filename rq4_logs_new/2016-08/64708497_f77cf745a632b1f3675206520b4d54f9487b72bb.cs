using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Mathmagician.Tests
{
    [TestClass]
    public class UserInterfaceTests
    {
        [TestMethod]
        public void UserInterface_EnsureICanCreateAnInstance()
        {
            //arrange: nothing in this case

            //act
            UserInterface my_userInterface = new UserInterface();

            //assert
            Assert.IsNotNull(my_userInterface);

        }

        [TestMethod]
        public void UserInterface_CanGetSetUserMathOperationCommand()
        {
            //arrange
            UserInterface my_userInterface = new UserInterface();

            //act
            my_userInterface.userMathOperationCommand = "hello";

            //assert
            Assert.AreEqual("hello", my_userInterface.userMathOperationCommand);
        }

        [TestMethod]
        public void UserInterface_CanGetSetUserNumbersToPrint()
        {
            //arrange
            UserInterface my_userInterface = new UserInterface();

            //act
            my_userInterface.userNumbersToPrint = "12";

            //assert
            Assert.AreEqual("12", my_userInterface.userNumbersToPrint);
        }
    }
}