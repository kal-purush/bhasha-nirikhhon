using DS.Contracts.OperationResponse;
using DS.Handlers.Abstract;
using DS.Handlers.Requests;
using DS.Handlers.Responses;
using DS.Handlers.Strategies.Factories;
using FluentValidation;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace DS.Handlers
{
    public class GetDeliveryCostHandler : HandlerBase<GetDeliveryCostHandlerRequest, GetDeliveryCostHandlerResponse>
    {
        public GetDeliveryCostHandler(
            ILogger<GetDeliveryCostHandler> logger,
            IValidator<GetDeliveryCostHandlerRequest> validator,
            IHandlerExceptionStrategyFactory handlerExceptionStrategyFactory)
            : base(logger, validator, handlerExceptionStrategyFactory)
        {
        }

        protected override async Task<IOperationResponse<GetDeliveryCostHandlerResponse>> HandleAsync(GetDeliveryCostHandlerRequest request)
        {
            throw new NotImplementedException();
        }
    }
}