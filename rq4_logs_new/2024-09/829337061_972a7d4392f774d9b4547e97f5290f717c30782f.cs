using ActionService.FunctionalTests.Setup;
using System.Net;

namespace ActionService.FunctionalTests
{
    [TestClass]
    public class ActionServiceCRUDTests : TestScenarioBase
    {
        [TestMethod]
        public void GetAllActions_ShouldReturnOK()
        {
            var client = GetClient();
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri("https://localhost:7120/api/v1/Action/GetAllActions"),
                Method = HttpMethod.Get
            };

            var response = client.SendAsync(request).Result;

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        }
    }
}