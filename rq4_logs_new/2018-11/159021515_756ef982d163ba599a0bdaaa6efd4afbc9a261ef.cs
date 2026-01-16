using DS.Handlers.Responses;
using FluentAssertions;
using System.Net;
using System.Net.Http;
using Xunit;

namespace DS.API.IntegrationTests.Controllers.V1
{
    public class DeliveryRoutesControllerGetTheCheapestCostTests : ControllerTestsBase<GetTheCheapestDeliveryCostHandlerResponse>, IClassFixture<TestFixture>
    {
        private const string _url = "api/v1/DeliveryRoutes";
        private readonly string _theCheapestCostUrlTemplate = $"{_url}/{{0}}/TheCheapestCost";
        private const string _invalidRoute = "AB";
        private const string _nonExistingRoute = "A-Z";

        public DeliveryRoutesControllerGetTheCheapestCostTests(TestFixture fixture)
            : base(fixture.Client)
        {
        }

        [Fact]
        public void GetTheCheapestCost_WhenRouteIsInvalid_ReturnsBadRequest()
        {
            GivenTheCheapestCostRequest(_invalidRoute);

            WhenRequestIsHandled();

            ThenResponseShouldBeUnsuccessfulWithStatusCode(HttpStatusCode.BadRequest);
            ThenPayloadShouldBeNull();
        }

        [Fact]
        public void GetTheCheapestCost_WhenRouteDoesNotExist_ReturnsNull()
        {
            GivenTheCheapestCostRequest(_nonExistingRoute);

            WhenRequestIsHandled();

            ThenResponseShouldBeSuccessful();
            ThenPayloadShouldBeCorrect(null);
        }

        [Theory]
        [InlineData("e-d", 9)]
        [InlineData("E-D", 9)]
        [InlineData("E-E", 6)]
        public void GetTheCheapestCost_WhenRouteExists_ReturnsTheCheapestCost(string route, int expected)
        {
            GivenTheCheapestCostRequest(route);

            WhenRequestIsHandled();

            ThenResponseShouldBeSuccessful();
            ThenPayloadShouldBeCorrect(expected);
        }

        #region Givens

        private void GivenTheCheapestCostRequest(string route) => Request = new HttpRequestMessage(HttpMethod.Get, string.Format(_theCheapestCostUrlTemplate, route));

        #endregion

        #region Thens

        private void ThenPayloadShouldBeCorrect(int? expected) => Response.Payload.TheCheapestDeliveryCost.Should().Be(expected);

        #endregion
    }
}