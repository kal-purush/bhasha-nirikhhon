using DS.Contracts.Handlers;
using DS.Contracts.OperationResponse;
using DS.Handlers.Abstract;
using DS.Handlers.UnitTests.Extensions;
using FluentAssertions;
using FluentAssertions.Execution;
using Microsoft.Extensions.Logging;
using System.Linq;
using System.Net;

namespace DS.Handlers.UnitTests
{
    public abstract class HandlerExceptionStrategyTestsBase<TRequest, TResponse>
        where TRequest : class, IHandlerRequest
        where TResponse : class, IHandlerResponse
    {
        protected HandlerExceptionStrategyBase<TRequest, TResponse> Handler;
        protected TRequest Request;
        protected IOperationResponse<TResponse> Response;
        protected ILogger Logger;

        #region Whens

        protected void WhenRequestIsHandled() => Response = Handler.Handle();

        #endregion

        #region Thens

        protected void ThenResponseShouldBeUnsuccessfulWithStatusCode(HttpStatusCode httpStatusCode)
        {
            using (new AssertionScope())
            {
                Response.IsSuccessful.Should().BeFalse();
                Response.StatusCode.Should().Be(httpStatusCode);
            }
        }

        protected void ThenResponseShouldBeSuccessful()
        {
            using (new AssertionScope())
            {
                Response.IsSuccessful.Should().BeTrue();
                Response.StatusCode.Should().Be(HttpStatusCode.OK);
            }
        }

        protected void ThenErrorMessageShouldBe(string errorMessage) => Response.Messages.Single().Should().Be(errorMessage);

        protected void ThenPayloadShouldBeNull() => Response.Payload.Should().BeNull();

        protected void ThenLoggerShouldBeCalledAndContainsMessage(LogLevel level, int numberOfCalls, string message)
        {
            Logger.ShouldBeCalledAndContainsMessage(level, numberOfCalls, message);
        }

        #endregion
    }
}