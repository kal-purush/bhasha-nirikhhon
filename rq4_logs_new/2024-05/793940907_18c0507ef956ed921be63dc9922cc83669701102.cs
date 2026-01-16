using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;
using ClassLibrary;

namespace Testing6
{

    [TestClass]
    public class TstSeller
    {
        private int sellerID = 1;
        private string firstName = "Muneeb";
        private string lastName  = "Muneeb",
        private Boolean isActive = true;
        private float commission = 3.3f;
        private float sales = 20.80f
        private DateTime createdAt = DateTime.Now.Date.toString();


        #region Attributs de tests supplémentaires
        //
        // Vous pouvez utiliser les attributs supplémentaires suivants lorsque vous écrivez vos tests :
        //
        // Utilisez ClassInitialize pour exécuter du code avant d'exécuter le premier test de la classe
        // [ClassInitialize()]
        // public static void MyClassInitialize(TestContext testContext) { }
        //
        // Utilisez ClassCleanup pour exécuter du code une fois que tous les tests d'une classe ont été exécutés
        // [ClassCleanup()]
        // public static void MyClassCleanup() { }
        //
        // Utilisez TestInitialize pour exécuter du code avant d'exécuter chaque test 
        // [TestInitialize()]
        // public void MyTestInitialize() { }
        //
        // Utilisez TestCleanup pour exécuter du code après que chaque test a été exécuté
        // [TestCleanup()]
        // public void MyTestCleanup() { }
        //
        #endregion

        [TestMethod]
        public void InstanceOK()
        {
            clsSeller seller = new clsSeller();
            Assert.IsNotNull(seller);
        }

        [TestMethod]
        public void SellerIdOk()
        {
            clsSeller seller = new clsSeller();
            seller.SellerID = sellerID;
            Assert.AreEqual(seller.SellerID, sellerID);
        }

        [TestMethod]
        public void FirstNameOK()
        {
            clsSeller seller = new clsSeller();
            seller.FirstName = firstName;
            Assert.AreEqual(seller.FirstName, firstName);
        }

        [TestMethod]
        public void LastNameOK()
        {
            clsSeller seller = new clsSeller();
            seller.LastName = lastName;
            Assert.AreEqual(seller.LastName, lastName);
        }

        [TestMethod]
        public void IsActiveOk()
        {
            clsSeller seller = new clsSeller();
            seller.IsActive = isActive;
            Assert.AreEqual(seller.IsActive, isActive);
        }

        [TestMethod]
        public void CommissionOk()
        {
            clsSeller seller = new clsSeller();
            seller.Commission = commission;
            Assert.AreEqual(seller.Commission, commission);
        }

        [TestMethod]
        public void SalesOk()
        {
            clsSeller seller = new clsSeller();
            seller.Sales = sales;
            Assert.AreEqual(seller.Sales, sales);
        }

        [TestMethod]
        public void CreatedAt()
        {
            clsSeller seller = new clsSeller();
            seller.CreatedAt = createdAt;
            Assert.AreEqual(seller.CreatedAt, createdAt);
        }
    }
}