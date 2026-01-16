using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Task4;

namespace UnitTestTask4

{
    [TestClass]
    public class UnitTest1
    {
        private TestContext testContext;

        public TestContext TestContext { get => testContext; set => testContext = value; }


        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.CSV",
                    "|DataDirectory|\\PFdata1.csv",
                    "PFdata1#csv",
                     DataAccessMethod.Sequential),
                     DeploymentItem("PFdata1.csv")
        ]
        public void PowerFunctionFindValueTest()
        {
            // F(x) = a*x^n

            //Arrange
            double a = double.Parse(TestContext.DataRow["a"].ToString());
            double x = double.Parse(TestContext.DataRow["x"].ToString());
            uint n = uint.Parse(TestContext.DataRow["n"].ToString());

            PowerFunction function = new PowerFunction(a, n);

            //Act
            double actual = function.FindFunctionValue(x);
            double result = double.Parse(TestContext.DataRow["result"].ToString());

            //Assert
            Assert.AreEqual(actual, result);

        }

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.CSV",
                    "|DataDirectory|\\PFdata2.csv",
                    "PFdata2#csv",
                     DataAccessMethod.Sequential),
                     DeploymentItem("PFdata2.csv")
        ]
        public void PowerFunctionFindDerivativeValueTest()
        {
            //Arrange
            double a = double.Parse(TestContext.DataRow["a"].ToString());
            double x = double.Parse(TestContext.DataRow["x"].ToString());
            uint n = uint.Parse(TestContext.DataRow["n"].ToString());
            uint derivativeOrder = uint.Parse(TestContext.DataRow["derivativeOrder"].ToString());

            PowerFunction function = new PowerFunction(a, n);

            //Act
            double actual = function.FindDerivativeValue(derivativeOrder, x);
            double result = double.Parse(TestContext.DataRow["result"].ToString());

            //Assert
            Assert.AreEqual(actual, result);
        }
        //********************TrigonometricFunctions********************

        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.CSV",
                   "|DataDirectory|\\TFdata1.csv",
                   "TFdata1#csv",
                    DataAccessMethod.Sequential),
                    DeploymentItem("TFdata1.csv")
       ]
        public void FindFunctionValueTest()
        {
            //Arrange
            uint name = uint.Parse(TestContext.DataRow["name"].ToString());
            double a = double.Parse(TestContext.DataRow["a"].ToString());
            double b = double.Parse(TestContext.DataRow["b"].ToString());
            uint x = uint.Parse(TestContext.DataRow["x"].ToString());
            
            TrigonometricFunction function = new TrigonometricFunction(x);

            //Act
            double actual = function.FindFunctionValue(x);
            double result = double.Parse(TestContext.DataRow["result"].ToString());

            //Assert
            Assert.AreEqual(actual, result);
        }
        [TestMethod]
        [DataSource("Microsoft.VisualStudio.TestTools.DataSource.CSV",
                  "|DataDirectory|\\TFdata2.csv",
                  "TFdata2#csv",
                   DataAccessMethod.Sequential),
                   DeploymentItem("TFdata2.csv")
      ]
        public void FindDerivativeValueTest()
        {
            //Arrange
            uint name = uint.Parse(TestContext.DataRow["name"].ToString());
            uint derivativeOrder = uint.Parse(TestContext.DataRow["derivativeOrder"].ToString());           
            uint x = uint.Parse(TestContext.DataRow["x"].ToString());

            TrigonometricFunction function = new TrigonometricFunction(derivativeOrder,x);

            //Act
            double actual = function.FindDerivativeValue(derivativeOrder, x);
            double result = double.Parse(TestContext.DataRow["result"].ToString());

            //Assert
            Assert.AreEqual(actual, result);
        }
        

    }
}