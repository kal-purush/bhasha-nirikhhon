using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using WundergroundNetLib;

namespace WundergroundNetTest
{
    [TestClass]
    public class GetDataTest
    {
        [TestMethod]
        public void GetDataAsyncTest()
        {
            // Arrange
            DataProvider dataProvider = new DataProvider();
            var forecast = dataProvider.GetDataAsync<WunForecast>(PwsGeographicLocation.Kaitaia_Northland_NZ, WunDataFeatures.forecast);
            string expectedResult = "http://www.wunderground.com/weather/api/d/terms.html";

            // Extra work for performance testing of Async vs Sync
            // IMPORTANT --- To really test this make sure to uncomment the Thread.Sleep(10000)
            // lines in the DownloadJsonString/DownloadJsonStringAsync to simulate a slow internet
            // connection and see how this effects performance.
            int incrementor = int.MinValue;
            for (int i = int.MinValue; i < int.MaxValue; i++)
            {
                incrementor++;
            }

            // Act
            string actualResult = forecast.Result.response.termsofService;

            // Assert
            Assert.AreEqual(expectedResult, actualResult);
        }

        [TestMethod]
        public void GetDataSyncTest()
        {
            // Arrange
            DataProvider dataProvider = new DataProvider();
            var forecast = dataProvider.GetData<WunForecast>(PwsGeographicLocation.Kaitaia_Northland_NZ, WunDataFeatures.forecast);
            string expectedResult = "http://www.wunderground.com/weather/api/d/terms.html";

            // Extra work for performance testing of Async vs Sync
            // IMPORTANT --- To really test this make sure to uncomment the Thread.Sleep(10000)
            // lines in the DownloadJsonString/DownloadJsonStringAsync to simulate a slow internet
            // connection and see how this effects performance.
            int incrementor = int.MinValue;
            for (int i = int.MinValue; i < int.MaxValue; i++)
            {
                incrementor++;
            }

            // Act
            string actualResult = forecast.response.termsofService;

            // Assert
            Assert.AreEqual(expectedResult, actualResult);
        }
    }
}