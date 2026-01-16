using Newtonsoft.Json;
using System.Net;
using System.Text;
using WarehouseService.FunctionalTests.Setup;
using WarehouseServiceAPI.Models;

namespace WarehouseService.FunctionalTests
{
    [TestClass]
    public class CRUD_NgTests : TestingScenarioBase
    {
        [TestMethod]
        public void GetPart_ShouldReturnNotFoundWithIdIfPartNotExists()
        {
            int requestedId = 2;
            var client = GetClient();
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(WarehouseServiceUri + $"GetPart/{requestedId}"),
                Method = HttpMethod.Get
            };

            var response = client.SendAsync(request).Result;

            Assert.AreEqual(HttpStatusCode.NotFound, response.StatusCode);
            var responseContent = response.Content.ReadAsStringAsync().Result;
            var returnedItem = int.Parse(responseContent);
            Assert.AreEqual(requestedId, returnedItem);
        }

        [DataTestMethod]
        [DataRow("Updated part", "Updated description", "Updated manufacturer", "Updated model", 2, 50, 25)] // Id not in database
        public void UpdatePart_ShouldReturnBadRequestIfDataNotCorrect(string partName,
            string partDescription,
            string partManufacturer,
            string partModel,
            int targetPartId,
            int partQuantity,
            int partMinStock)
        {
            Part updadetPart = new(partName, partDescription, partManufacturer, partModel, partQuantity, partMinStock)
            {
                Id = targetPartId
            };

            var client = GetClient();
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(WarehouseServiceUri + "UpdatePart"),
                Method = HttpMethod.Put,
                Content = new StringContent(JsonConvert.SerializeObject(updadetPart), Encoding.UTF8, "application/json")
            };

            var result = client.SendAsync(request);

            Assert.AreEqual(HttpStatusCode.BadRequest, result.Result.StatusCode);
        }

        [TestMethod]
        public void DeletePart_ShouldReturnBadRequestWithMessageIfPartNotExists()
        {
            int targetPartId = 2;
            var client = GetClient();
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(WarehouseServiceUri + $"DeletePart/{targetPartId}"),
                Method = HttpMethod.Delete
            };

            var result = client.SendAsync(request);

            Assert.AreEqual(HttpStatusCode.BadRequest, result.Result.StatusCode);

            var newItem = client.DownloadPart(targetPartId);
            Assert.IsTrue(newItem is null);
        }
    }
}