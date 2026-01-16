using DS.Contracts.Handlers;
using DS.Contracts.OperationResponse;
using DS.Dtos.Exceptions;
using DS.Handlers.Abstract;
using Microsoft.Extensions.Logging;
using System.Linq;

namespace DS.Handlers.Strategies.Exceptions
{
    public class HandlerDomainModelExceptionStrategy<TRequest, TResponse> : HandlerExceptionStrategyBase<TRequest, TResponse>
        where TRequest : class, IHandlerRequest
        where TResponse : class, IHandlerResponse
    {
        private readonly DomainModelException _exception;

        public HandlerDomainModelExceptionStrategy(TRequest request, ILogger logger, DomainModelException exception)
            : base(request, logger)
        {
            _exception = exception;
        }

        public override IOperationResponse<TResponse> Handle()
        {
            var errorMessages = _exception.ErrorMessages.ToArray();
            LogWarning(_exception, errorMessages);

            return BadRequest(errorMessages);
        }
    }
}