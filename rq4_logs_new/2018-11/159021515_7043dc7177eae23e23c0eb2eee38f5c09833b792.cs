using DS.Dtos.Exceptions;
using DS.Handlers.Abstract;
using DS.Handlers.Requests;
using DS.Handlers.Responses;
using DS.Handlers.Strategies.Exceptions;
using DS.Handlers.Strategies.Factories;
using FluentAssertions;
using System;
using System.Collections.Generic;
using Xunit;

namespace DS.Handlers.UnitTests.Strategies.Factories
{
    public class HandlerExceptionStrategyFactoryTests
    {
        public static IEnumerable<object[]> ValidData =>
            new List<object[]>
            {
                new object[]
                {
                    new DomainModelException(),
                    typeof(HandlerDomainModelExceptionStrategy<GetDeliveryCostHandlerRequest, GetDeliveryCostHandlerResponse>)
                },
                new object[]
                {
                    new DataAccessNotFoundException(),
                    typeof(HandlerDataAccessNotFoundExceptionStrategy<GetDeliveryCostHandlerRequest, GetDeliveryCostHandlerResponse>)
                },
                new object[]
                {
                    new Exception(),
                    typeof(HandlerExceptionStrategy<GetDeliveryCostHandlerRequest, GetDeliveryCostHandlerResponse>)
                },
                new object[]
                {
                    new ArgumentOutOfRangeException(),
                    typeof(HandlerExceptionStrategy<GetDeliveryCostHandlerRequest, GetDeliveryCostHandlerResponse>)
                }
            };

        private readonly IHandlerExceptionStrategyFactory _factory;
        private Exception _exception;
        private Func<HandlerExceptionStrategyBase<GetDeliveryCostHandlerRequest, GetDeliveryCostHandlerResponse>> _func;

        public HandlerExceptionStrategyFactoryTests()
        {
            _factory = new HandlerExceptionStrategyFactory();
        }

        [Fact]
        public void Create_WhenCreatingWithNullException_ThrowsInvalidArgumentException()
        {
            GivenNullException();

            WhenExceptionStrategyIsCreated();

            ThenExceptionShouldBeThrownWithMessage();
        }

        [Theory]
        [MemberData(nameof(ValidData))]
        public void Create_WhenCreatingWithValidException_ReturnsHandlerExceptionStrategy(Exception exception, Type expected)
        {
            GivenException(exception);

            WhenExceptionStrategyIsCreated();

            ThenExceptionStrategyShouldBeCreatedCorrectly(expected);
        }

        #region Givens

        private void GivenNullException() => _exception = null;

        private void GivenException(Exception exception) => _exception = exception;

        #endregion

        #region Whens

        private void WhenExceptionStrategyIsCreated()
        {
            _func = (() => _factory.Create<GetDeliveryCostHandlerRequest, GetDeliveryCostHandlerResponse>(null, null, _exception));
        }

        #endregion

        #region Thens

        private void ThenExceptionShouldBeThrownWithMessage()
        {
            _func
                .Should().Throw<ArgumentNullException>()
                .Which.Message.Should().Contain("Value cannot be null.")
                .And.Contain("Parameter name: exception");
        }

        private void ThenExceptionStrategyShouldBeCreatedCorrectly(Type expected) => _func().Should().BeOfType(expected);

        #endregion
    }
}