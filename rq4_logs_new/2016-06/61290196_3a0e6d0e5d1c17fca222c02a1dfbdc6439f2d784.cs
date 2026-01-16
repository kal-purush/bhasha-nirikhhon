using StupidCalculatorLib;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace TestLib
{
    
    
    /// <summary>
    ///Classe de test pour KeyBinderTest, destinée à contenir tous
    ///les tests unitaires KeyBinderTest
    ///</summary>
    [TestClass()]
    public class KeyBinderTest
    {
        private TestContext testContextInstance;

        /// <summary>
        ///Obtient ou définit le contexte de test qui fournit
        ///des informations sur la série de tests active ainsi que ses fonctionnalités.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        #region Attributs de tests supplémentaires
        // 
        //Vous pouvez utiliser les attributs supplémentaires suivants lorsque vous écrivez vos tests :
        //
        //Utilisez ClassInitialize pour exécuter du code avant d'exécuter le premier test dans la classe
        //[ClassInitialize()]
        //public static void MyClassInitialize(TestContext testContext)
        //{
        //}
        //
        //Utilisez ClassCleanup pour exécuter du code après que tous les tests ont été exécutés dans une classe
        //[ClassCleanup()]
        //public static void MyClassCleanup()
        //{
        //}
        //
        //Utilisez TestInitialize pour exécuter du code avant d'exécuter chaque test
        //[TestInitialize()]
        //public void MyTestInitialize()
        //{
        //}
        //
        //Utilisez TestCleanup pour exécuter du code après que chaque test a été exécuté
        //[TestCleanup()]
        //public void MyTestCleanup()
        //{
        //}
        //
        #endregion

        /// <summary>
        ///Test pour op2char
        ///</summary>
        [TestMethod()]
        [DeploymentItem("StupidCalculatorLib.dll")]
        public void op2charTest()
        {
            Assert.AreEqual('+', KeyBinder.op2char(Operation.ADD));
            Assert.AreEqual('-', KeyBinder.op2char(Operation.SUBSTRACT));
            Assert.AreEqual('×', KeyBinder.op2char(Operation.MULTIPLY));
        }

        /// <summary>
        ///Test pour pushButton
        ///</summary>
        [TestMethod()]
        public void pushButtonTest()
        {
            KeyBinder binder = new KeyBinder();
            binder.pushButton('2');
            Assert.AreEqual("25", binder.pushButton('5'));
            binder.pushButton('x');
            Assert.AreEqual("25 × 5", binder.pushButton('5'));
            Assert.AreEqual("125", binder.pushButton('='));
            binder.pushButton('-');
            binder.pushButton('5');
            Assert.AreEqual("120", binder.pushButton('='));
            binder.pushButton('/');
            binder.pushButton('1');
            binder.pushButton('0');
            Assert.AreEqual("12", binder.pushButton('='));
        }

        [TestMethod()]
        public void decimalTest()
        {
            KeyBinder binder = new KeyBinder();
            binder.pushButton(',');
            binder.pushButton('9');
            Assert.AreEqual("0,95", binder.pushButton('5'));
            binder.pushButton('+');
            binder.pushButton(',');
            binder.pushButton('0');
            binder.pushButton('5');
            Assert.AreEqual("1", binder.pushButton('='));
            binder.pushButton(',');
            binder.pushButton('5');
            Assert.AreEqual("0,5", binder.pushButton('='));
        }
    }
}