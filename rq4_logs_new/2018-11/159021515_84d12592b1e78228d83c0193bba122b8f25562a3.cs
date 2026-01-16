using DS.Handlers.Responses;
using FluentAssertions;
using System.Net;
using System.Net.Http;
using Xunit;

namespace DS.API.IntegrationTests.Controllers.V1
{
    public class DeliveryRoutesControllerGetPossibleTests : ControllerTestsBase<GetPossibleDeliveryRoutesHandlerResponse>, IClassFixture<TestFixture>
    {
        private const string _url = "api/v1/DeliveryRoutes";
        private readonly string _possibleUrlTemplate = $"{_url}/{{0}}/Possible?MaxRouteRepeat={{1}}&";
        private const string _invalidRoute = "AB";
        private const string _nonExistingRoute = "A-Z";

        public DeliveryRoutesControllerGetPossibleTests(TestFixture fixture)
            : base(fixture.Client)
        {
        }

        [Fact]
        public void GetPossible_WhenRouteIsInvalid_ReturnsBadRequest()
        {
            GivenPossibleRequest(_invalidRoute);

            WhenRequestIsHandled();

            ThenResponseShouldBeUnsuccessfulWithStatusCode(HttpStatusCode.BadRequest);
            ThenPayloadShouldBeNull();
        }

        [Fact]
        public void GetPossible_WhenRouteDoesNotExist_ReturnsZero()
        {
            GivenPossibleRequest(_nonExistingRoute);

            WhenRequestIsHandled();

            ThenResponseShouldBeSuccessful();
            ThenPayloadShouldBeCorrect(0);
        }

        [Theory]
        [InlineData("e-d", 1, null, 4, 4)]
        [InlineData("E-D", 1, null, 4, 4)]
        [InlineData("E-E", 1, null, null, 5)]
        [InlineData("E-E", 2, 20, null, 29)]
        public void GetPossible_WhenRouteExists_ReturnsPossible(string route, int maxRouteRepeat, int? maxDeliveryCost, int? maxStop, int expected)
        {
            GivenPossibleRequest(route, maxRouteRepeat, maxDeliveryCost, maxStop);

            WhenRequestIsHandled();

            ThenResponseShouldBeSuccessful();
            ThenPayloadShouldBeCorrect(expected);
        }

        #region Givens

        private void GivenPossibleRequest(string route, int maxRouteRepeat = 1, int? maxDeliveryCost = null, int? maxStop = null)
        {
            var url = string.Format(_possibleUrlTemplate, route, maxRouteRepeat);

            if (maxDeliveryCost.HasValue)
                url += $"&MaxDeliveryCost={maxDeliveryCost}";

            if (maxStop.HasValue)
                url += $"&maxStop={maxStop}";

            Request = new HttpRequestMessage(HttpMethod.Get, url);
        }

        #endregion

        #region Thens

        private void ThenPayloadShouldBeCorrect(int expected) => Response.Payload.PossibleRoute.Should().Be(expected);

        #endregion
    }
}