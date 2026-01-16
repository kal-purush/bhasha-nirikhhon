using ActionServiceAPI.Application.Action.Commands.CreateActionCommand;
using ActionServiceAPI.Application.DataTransferObjects.Models;
using Newtonsoft.Json;
using System.Net;

namespace ActionService.FunctionalTests.Setup
{
    public static class ActionApiHttpClientExtensions
    {
        public static ActionDto? DownloadAction(this HttpClient client, int id)
        {
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(TestScenarioBase.ActionServiceUri + $"GetAction/{id}"),
                Method = HttpMethod.Get
            };

            var response = client.SendAsync(request).Result;

            if (response.StatusCode != HttpStatusCode.OK)
                return null;

            var responseContent = response.Content.ReadAsStringAsync().Result;
            return JsonConvert.DeserializeObject<ActionDto>(responseContent);
        }

        public static int? AddExampleAction(this HttpClient client)
        {
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(TestScenarioBase.ActionServiceUri + "CreateAction"),
                Method = HttpMethod.Post,
                Headers =
                {
                    {HttpRequestHeader.ContentType.ToString(), "application/json"},
                },
                Content = JsonContent.Create(new CreateActionCommand(
                    "Example",
                    "Example",
                    DateTime.Now,
                    DateTime.Now,
                    TestScenarioBase.FirstEmployeeId,
                    TestScenarioBase.SecondEmployeeId,
                    [new SparePartDto() { PartId = TestScenarioBase.AvailableSparePartId, Quantity = TestScenarioBase.AvailableSparePartQuantity / 2 }]))
            };

            var response = client.SendAsync(request).Result;
            var returnedId = int.Parse(response.Content.ReadAsStringAsync().Result);

            if (response.StatusCode != HttpStatusCode.OK)
                return null;

            return returnedId;
        }
    }
}