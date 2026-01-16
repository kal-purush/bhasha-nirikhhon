using System;
using Jojatekok.PoloniexAPI;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace PoloniexApi.Net.Tests.Integration
{
    [TestClass]
    public class MarketTests
    {
        IMarkets CreateTarget()
        {
            return TestHelpers.CreateClient().Markets;
        }

        public TestContext TestContext { get; set; }

        [TestMethod]
        public void GetAllOrders_ReturnsValues()
        {
            var actual = CreateTarget().GetAllOpenOrdersAsync(10).Result;
            TestContext.WriteLine("Found {0} items", actual.Count);
        }      
    }
}