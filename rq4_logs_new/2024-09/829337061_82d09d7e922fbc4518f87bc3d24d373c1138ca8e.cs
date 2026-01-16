using ActionService.FunctionalTests.Setup;
using ActionServiceAPI.Application.Action.Commands.CreateActionCommand;
using ActionServiceAPI.Application.DataTransferObjects.Models;
using Newtonsoft.Json;
using System.Net;

namespace ActionService.FunctionalTests
{
    [TestClass]
    public class CRUD_OkTests : TestScenarioBase
    {
        const string ActionServiceUri = "https://localhost:7120/api/v1/Action/";

        [TestMethod]
        public void GetAllActions_ShouldReturnOK()
        {
            var client = GetClient();
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(ActionServiceUri + "GetAllActions"),
                Method = HttpMethod.Get
            };

            var response = client.SendAsync(request).Result;
            var returnedItem = JsonConvert.DeserializeObject<List<ActionDto>>(response.Content.ReadAsStringAsync().Result);

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            Assert.IsTrue(returnedItem is not null);
            Assert.AreEqual(1, returnedItem.Count);
        }

        [TestMethod]
        public void GetAction_ShouldReturnExistingItem()
        {
            var client = GetClient();
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(ActionServiceUri + "GetAction/1"),
                Method = HttpMethod.Get
            };

            var response = client.SendAsync(request).Result;
            var returnedItem = JsonConvert.DeserializeObject<ActionDto>(response.Content.ReadAsStringAsync().Result);

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            Assert.IsTrue(returnedItem is not null);
            Assert.IsTrue(returnedItem.Id == 1);
        }

        [TestMethod]
        public void CreateAction_ShouldReturnOKAndId()
        {
            var client = GetClient();
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(ActionServiceUri + "CreateAction"),
                Method = HttpMethod.Post,
                Headers =
                {
                    {HttpRequestHeader.ContentType.ToString(), "application/json"},
                },
                Content = JsonContent.Create(new CreateActionCommand("New", "new", DateTime.Now, DateTime.Now, "TestEmployee", "TestEmployee", [new SparePartDto() { PartId = 1, Quantity = 5 }]))
            };

            var response = client.SendAsync(request).Result;
            var returnedItem = JsonConvert.DeserializeObject<int>(response.Content.ReadAsStringAsync().Result);

            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            Assert.AreEqual(2, returnedItem);
        }
    }
}