using ClassLibrary;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;

namespace Testing6
{

    [TestClass]
    public class TstSellerUser
    {
        [TestMethod]
        public void InstanceOk()
        {
            clsSellerUser AnUser = new clsSellerUser();
            Assert.IsNotNull(AnUser);
        }

        [TestMethod]
        public void UserIDPropertyOk()
        {
            clsSellerUser AnUser = new clsSellerUser();
            int TestData = 1;
            AnUser.UserID = TestData;
            Assert.AreEqual(AnUser.UserID, TestData);
        }

        [TestMethod]
        public void UserNamePropertyOk()
        {
            clsSellerUser AnUser = new clsSellerUser();
            string TestDate = "Muneeb";
            AnUser.UserName = TestDate;
            Assert.AreEqual(AnUser.UserName, TestDate);
        }

        [TestMethod]
        public void PasswordPropertyOk()
        {
            clsSellerUser AnUser = new clsSellerUser();
            string TestDate = "Muneeb234";
            AnUser.Password = TestDate;
            Assert.AreEqual(AnUser.Password, TestDate);
        }

        [TestMethod]
        public void FindUserMethodOk()
        {
            clsSellerUser AnUser = new clsSellerUser();
            Boolean Found = false;
            string UserName = "Muneeb";
            string Password = "Muneeb234";
            Found = AnUser.FindUser(UserName, Password);
            Assert.IsTrue(Found);
        }

        [TestMethod]
        public void TestUserNamePWFound()
        {
            clsSellerUser AnUser = new clsSellerUser();
            Boolean Found = false;
            Boolean OK = true;
            string UserName = "Muneeb";
            string Password = "Muneeb234";
            Found = AnUser.FindUser(UserName, Password);
            if (AnUser.UserName != UserName && AnUser.Password != Password)
            {
                OK = false;
            }
            Assert.IsTrue(OK);
        }
    }
}