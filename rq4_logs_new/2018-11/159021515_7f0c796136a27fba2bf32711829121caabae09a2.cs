using DS.Contracts.DataAccess.Repositories;
using DS.Handlers.Requests;
using DS.Handlers.Responses;
using DS.Handlers.Strategies.Factories;
using DS.Handlers.Validators;
using Microsoft.Extensions.Logging;
using NSubstitute;
using System.Net;
using Xunit;

namespace DS.Handlers.UnitTests
{
    public class GetDeliveryCostHandlerTests : HandlerTestsBase<GetDeliveryCostHandler, GetDeliveryCostHandlerRequest, GetDeliveryCostHandlerResponse>
    {
        private const string _routeShouldNotBeEmpty = "'Route' should not be empty.";

        private readonly IDeliveryRouteReadOnlyRepository _deliveryRouteReadOnlyRepository;

        public GetDeliveryCostHandlerTests()
        {
            _deliveryRouteReadOnlyRepository = Substitute.For<IDeliveryRouteReadOnlyRepository>();
            Logger = Substitute.For<ILogger<GetDeliveryCostHandler>>();
            Handler = new GetDeliveryCostHandler(_deliveryRouteReadOnlyRepository, Logger, new GetDeliveryCostHandlerRequestValidator(), new HandlerExceptionStrategyFactory());
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("  ")]
        public void HandleAsync_WhenRouteIsNullOrEmpty_ReturnsNull(string route)
        {
            GivenRequest(route);

            WhenRequestIsHandled();

            ThenResponseShouldBeUnsuccessfulWithStatusCode(HttpStatusCode.BadRequest);
            ThenErrorMessageShouldBe(_routeShouldNotBeEmpty);
            ThenPayloadShouldBeNull();
            ThenLoggerShouldBeCalledAndContainsMessage(LogLevel.Warning, 1, _routeShouldNotBeEmpty);
            ThenBookingRepositoryShouldNotBeCalled();
        }

        #region Givens

        private void GivenRequest(string route) => Request = new GetDeliveryCostHandlerRequest(route);

        #endregion

        #region Thens

        private void ThenBookingRepositoryShouldNotBeCalled() => _deliveryRouteReadOnlyRepository.DidNotReceive();

        #endregion
    }
}