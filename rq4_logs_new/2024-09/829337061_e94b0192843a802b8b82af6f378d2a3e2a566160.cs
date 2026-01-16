using ActionService.FunctionalTests.Setup;
using ActionServiceAPI.Application.Action.Commands.CreateActionCommand;
using ActionServiceAPI.Application.Action.Commands.DeleteActionCommand;
using ActionServiceAPI.Application.Action.Commands.UpdateActionCommand;
using ActionServiceAPI.Application.DataTransferObjects.Models;
using System.Net;

namespace ActionService.FunctionalTests
{
    [TestClass]
    public class CRUD_NgTests : TestScenarioBase
    {
        [TestMethod]
        public void GetAction_ShouldReturnNotFound()
        {
            int id = 100;

            var client = GetClient();
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(ActionServiceUri + $"GetAction/{id}"),
                Method = HttpMethod.Get
            };

            var response = client.SendAsync(request).Result;
            var responseContent = response.Content.ReadAsStringAsync().Result;
            var returnedItem = int.Parse(responseContent);

            Assert.AreEqual(HttpStatusCode.NotFound, response.StatusCode);
            Assert.IsTrue(returnedItem == id);
        }

        [DataTestMethod]
        [DataRow("Name", "Desc", 2024, 09, 20, 2024, 09, 22, "first", "second", 1, 5, nameof(CreateActionCommand.CreatedBy), nameof(CreateActionCommand.ConductedBy))]
        [DataRow("Name", "Desc", 2024, 09, 20, 2024, 09, 22, FirstEmployeeId, "second", 1, 5, nameof(CreateActionCommand.ConductedBy))]
        [DataRow("Name", "Desc", 2024, 09, 20, 2024, 09, 22, "first", FirstEmployeeId, 1, 5, nameof(CreateActionCommand.CreatedBy))]
        [DataRow("Name", "Desc", 2024, 09, 20, 2024, 09, 22, FirstEmployeeId, SecondEmployeeId, 2, 5, nameof(CreateActionCommand.Parts), "not found")]
        [DataRow("Name", "Desc", 2024, 09, 20, 2024, 09, 22, FirstEmployeeId, SecondEmployeeId, 1, 11, nameof(CreateActionCommand.Parts), "enough")]
        public void CreateAction_ShouldReturnBadRequestAndValidationMessages(
            string name, 
            string description, 
            int startYear,
            int startMonth,
            int startDay,
            int endYear,
            int endMonth,
            int endDay,
            string createdBy, 
            string conductedBy, 
            int partId, 
            int partQuantity,
            params string[] expectedMessage)
        {
            DateTime startDate = new(startYear, startMonth, startDay);
            DateTime endDate = new(endYear, endMonth, endDay);
            List<SparePartDto> parts = [new SparePartDto() { PartId = partId, Quantity = partQuantity }];

            var client = GetClient();
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(ActionServiceUri + "CreateAction"),
                Method = HttpMethod.Post,
                Headers =
                {
                    {HttpRequestHeader.ContentType.ToString(), "application/json"},
                },
                Content = JsonContent.Create(new CreateActionCommand(name, description, startDate, endDate, createdBy, conductedBy, parts))
            };

            var response = client.SendAsync(request).Result;
            var responseContent = response.Content.ReadAsStringAsync().Result;

            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);

            foreach (var keyWord in expectedMessage)
                Assert.IsTrue(responseContent.Contains(keyWord));
        }

        [DataTestMethod]
        [DataRow(2, "Name", "Desc", 2024, 09, 20, 2024, 09, 22, FirstEmployeeId, SecondEmployeeId, 1, 5, nameof(UpdateActionCommand.Id))]
        [DataRow(1, "Name", "Desc", 2024, 09, 20, 2024, 09, 22, "first", "second", 1, 5, nameof(UpdateActionCommand.CreatedBy), nameof(UpdateActionCommand.ConductedBy))]
        [DataRow(1, "Name", "Desc", 2024, 09, 20, 2024, 09, 22, FirstEmployeeId, "second", 1, 5, nameof(UpdateActionCommand.ConductedBy))]
        [DataRow(1, "Name", "Desc", 2024, 09, 20, 2024, 09, 22, "first", FirstEmployeeId, 1, 5, nameof(UpdateActionCommand.CreatedBy))]
        [DataRow(1, "Name", "Desc", 2024, 09, 20, 2024, 09, 22, FirstEmployeeId, SecondEmployeeId, 2, 5, nameof(UpdateActionCommand.Parts), "not found")]
        [DataRow(1, "Name", "Desc", 2024, 09, 20, 2024, 09, 22, FirstEmployeeId, SecondEmployeeId, 1, 25, nameof(UpdateActionCommand.Parts), "enough")]
        public void UpdateAction_ShouldReturnBadRequestAndValidationMessages(
            int id,
            string name,
            string description,
            int startYear,
            int startMonth,
            int startDay,
            int endYear,
            int endMonth,
            int endDay,
            string createdBy,
            string conductedBy,
            int partId,
            int partQuantity,
            params string[] expectedMessage)
        {
            DateTime startDate = new(startYear, startMonth, startDay);
            DateTime endDate = new(endYear, endMonth, endDay);
            List<SparePartDto> parts = [new SparePartDto() { PartId = partId, Quantity = partQuantity }];

            var client = GetClient();
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(ActionServiceUri + "UpdateAction"),
                Method = HttpMethod.Put,
                Headers =
                {
                    {HttpRequestHeader.ContentType.ToString(), "application/json"},
                },
                Content = JsonContent.Create(new UpdateActionCommand(id, name, description, startDate, endDate, createdBy, conductedBy, parts))
            };

            var response = client.SendAsync(request).Result;
            var responseContent = response.Content.ReadAsStringAsync().Result;

            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);

            foreach (var keyWord in expectedMessage)
                Assert.IsTrue(responseContent.Contains(keyWord));
        }

        [TestMethod]
        public void DeleteAction_ShouldReturnNotFoundAndId()
        {
            int id = 2;

            var client = GetClient();
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(ActionServiceUri + $"DeleteAction/{id}"),
                Method = HttpMethod.Delete,
                Headers =
                {
                    {HttpRequestHeader.ContentType.ToString(), "application/json"},
                },
                Content = JsonContent.Create(new DeleteActionCommand(id))
            };

            var response = client.SendAsync(request).Result;
            var returnedId = int.Parse(response.Content.ReadAsStringAsync().Result);

            Assert.AreEqual(HttpStatusCode.NotFound, response.StatusCode);
            Assert.AreEqual(id, returnedId);
        }
    }
}