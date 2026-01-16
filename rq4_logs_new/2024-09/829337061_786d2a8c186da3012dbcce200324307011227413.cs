using Newtonsoft.Json;
using System.Net;
using System.Text;
using WarehouseService.FunctionalTests.Setup;
using WarehouseServiceAPI.Models;

namespace WarehouseService.FunctionalTests
{
    [TestClass]
    public class CRUD_OkTests : TestingScenarioBase
    {
        [TestMethod]
        public void GetAllParts_ShouldReturnOk()
        {
            var client = GetClient();
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(WarehouseServiceUri + "GetAllParts"),
                Method = HttpMethod.Get
            };

            var response = client.SendAsync(request).Result;

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            var responseContent = response.Content.ReadAsStringAsync().Result;
            var returnedItem = JsonConvert.DeserializeObject<List<Part>>(responseContent);
            Assert.IsTrue(returnedItem is not null);
            Assert.AreEqual(1, returnedItem.Count);
        }

        [TestMethod]
        public void GetPart_ShouldReturnOkIfPartExists()
        {
            int requestedId = 1;
            var client = GetClient();
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(WarehouseServiceUri + $"GetPart/{requestedId}"),
                Method = HttpMethod.Get
            };

            var response = client.SendAsync(request).Result;

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            var responseContent = response.Content.ReadAsStringAsync().Result;
            var returnedItem = JsonConvert.DeserializeObject<Part>(responseContent);
            Assert.IsTrue(returnedItem is not null);
            Assert.AreEqual(requestedId, returnedItem.Id);
        }

        [TestMethod]
        public void AddPart_ShouldReturnOkAndNewIdIfDataCorrect()
        {
            string partName = "New part",
                partDescription = "New description",
                partManufacturer = "New manufacturer",
                partModel = "New model";
            int partQuantity = 10,
                partMinStock = 5;

            Part newPart = new(partName, partDescription, partManufacturer, partModel, partQuantity, partMinStock);
            var client = GetClient();
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(WarehouseServiceUri + "AddPart"),
                Method = HttpMethod.Post,
                Content = new StringContent(JsonConvert.SerializeObject(newPart), Encoding.UTF8, "application/json")
            };

            var result = client.SendAsync(request);

            Assert.AreEqual(HttpStatusCode.OK, result.Result.StatusCode);
            var responseContent = result.Result.Content.ReadAsStringAsync().Result;
            var newId = JsonConvert.DeserializeObject<int>(responseContent);
            Assert.IsTrue(newId != 1);

            var newItem = client.DownloadPart(newId);
            Assert.IsTrue(newItem is not null);
            Assert.AreEqual(newId, newItem.Id);
            Assert.AreEqual(partName, newItem.Name);
            Assert.AreEqual(partDescription, newItem.Description);
            Assert.AreEqual(partManufacturer, newItem.Manufacturer);
            Assert.AreEqual(partModel, newItem.Model);
            Assert.AreEqual(partQuantity, newItem.QuantityOnStock);
            Assert.AreEqual(partMinStock, newItem.MinimumStock);
        }

        [TestMethod]
        public void UpdatePart_ShouldReturnOkIfDataCorrect()
        {
            string partName = "Updated part",
                partDescription = "Updated description",
                partManufacturer = "Updated manufacturer",
                partModel = "Updated model";
            int targetPartId = 1,
                partQuantity = 50,
                partMinStock = 25;

            Part updadetPart = new(partName, partDescription, partManufacturer, partModel, partQuantity, partMinStock)
            {
                Id = 1
            };

            var client = GetClient();
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(WarehouseServiceUri + "UpdatePart"),
                Method = HttpMethod.Put,
                Content = new StringContent(JsonConvert.SerializeObject(updadetPart), Encoding.UTF8, "application/json")
            };

            var result = client.SendAsync(request);

            Assert.AreEqual(HttpStatusCode.OK, result.Result.StatusCode);

            var updatedItem = client.DownloadPart(targetPartId);
            Assert.IsTrue(updatedItem is not null);
            Assert.AreEqual(targetPartId, updatedItem.Id);
            Assert.AreEqual(partName, updatedItem.Name);
            Assert.AreEqual(partDescription, updatedItem.Description);
            Assert.AreEqual(partManufacturer, updatedItem.Manufacturer);
            Assert.AreEqual(partModel, updatedItem.Model);
            Assert.AreEqual(partQuantity, updatedItem.QuantityOnStock);
            Assert.AreEqual(partMinStock, updatedItem.MinimumStock);
        }

        [TestMethod]
        public void DeletePart_ShouldReturnOkIfPartExisted()
        {
            int targetPartId = 1;
            var client = GetClient();
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(WarehouseServiceUri + $"DeletePart/{targetPartId}"),
                Method = HttpMethod.Delete
            };

            var result = client.SendAsync(request);

            Assert.AreEqual(HttpStatusCode.OK, result.Result.StatusCode);

            var newItem = client.DownloadPart(targetPartId);
            Assert.IsTrue(newItem is null);
        }
    }
}