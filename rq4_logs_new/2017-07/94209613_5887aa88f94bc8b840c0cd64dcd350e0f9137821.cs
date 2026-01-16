using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Screen2.BLL;
using Screen2.BLL.Interface;
using Screen2.DAL.Interface;

namespace Screen2.Tests.BLL
{
    [TestClass]
    public class TickerBLLTest
    {
        private Mock<IUnitWork> unitMock = new Mock<IUnitWork>();
        private Mock<ITickerLoader> tickerMock = new Mock<ITickerLoader>();

        //TickerBLLTest()
        //{
        //    tickerMock.Setup(loader => loader.LoadTickers()).Returns(true);
        //}

        [ClassInitialize]
        public static void SetUp(TestContext context)
        {
        }
        
        [TestInitialize]
        public void Initialize()
        {
            tickerMock.Setup(loader => loader.LoadTickers()).Returns(true);
        }


        [TestMethod]
        public void TestLoadTickers_Should_Return_True()
        {
            TickerBLL tbll = new TickerBLL(unitMock.Object);

            Boolean result = tbll.LoadTickers(tickerMock.Object);

            Assert.IsTrue(result);
        }
    }
}