using DS.API.IntegrationTests.OperationResponse;
using DS.Contracts.Handlers;
using FluentAssertions;
using FluentAssertions.Execution;
using Newtonsoft.Json;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace DS.API.IntegrationTests.Controllers
{
    public abstract class ControllerTestsBase<TResponse>
        where TResponse : class, IHandlerResponse
    {
        protected readonly HttpClient Client;
        protected HttpRequestMessage Request;
        protected TestOperationResponse<TResponse> Response;

        protected ControllerTestsBase(HttpClient client)
        {
            Client = client;
        }

        #region Whens

        protected void WhenRequestIsHandled() => Response = Task.Run(async () => await GetAsync(Request)).Result;

        #endregion

        #region Thens

        protected void ThenPayloadShouldBeNull() => Response.Payload.Should().BeNull();

        protected void ThenResponseShouldBeUnsuccessfulWithStatusCode(HttpStatusCode httpStatusCode)
        {
            using (new AssertionScope())
            {
                Response.IsSuccessful.Should().BeFalse();
                Response.StatusCode.Should().Be(httpStatusCode);
            }
        }

        protected void ThenResponseShouldContainMessage(string message)
        {
            Response.Messages.Single().Should().Be(message);
        }

        protected void ThenResponseShouldBeSuccessful()
        {
            using (new AssertionScope())
            {
                Response.IsSuccessful.Should().BeTrue();
                Response.StatusCode.Should().Be(HttpStatusCode.OK);
            }
        }

        #endregion

        #region Private Methods

        private async Task<TestOperationResponse<TResponse>> GetAsync(HttpRequestMessage httpRequestMessage)
        {
            var httpResponseMessage = await Client.SendAsync(httpRequestMessage);
            var content = await httpResponseMessage.Content.ReadAsStringAsync();

            return JsonConvert.DeserializeObject<TestOperationResponse<TResponse>>(content);
        }

        #endregion
    }
}