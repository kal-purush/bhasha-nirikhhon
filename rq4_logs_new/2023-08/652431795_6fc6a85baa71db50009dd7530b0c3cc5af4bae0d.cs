using AutoMapper;
using MediatR;
using Microsoft.AspNetCore.Mvc;
using Nameless.DailyDos.Web.Api.v1.Models.Output;
using Nameless.DailyDos.Web.Domain.Requests;
using Nameless.Web.Infrastructure;

namespace Nameless.DailyDos.Web.Api.v1.Endpoints {
    public sealed class Get : IMinimalEndpoint {
        #region Public Static Methods

        public static async Task<IResult> HandleAsync([FromRoute] Guid id, IMediator mediator, IMapper mapper, CancellationToken cancellationToken) {
            var request = new GetTodoItemRequest {
                Id = id
            };
            var dto = await mediator.Send(request, cancellationToken);
            var output = mapper.Map<TodoItemOutput?>(dto);

            return output is not null
                ? Results.Ok(output)
                : Results.NotFound();
        }

        #endregion

        #region IMinimalEndpoint Members

        public string Name => nameof(Get);

        public string Summary => "Get a specific To-Do";

        public string Description => "Get a specific To-Do";

        public string Group => "To-Do";

        public int Version => 1;

        IEndpointConventionBuilder IMinimalEndpoint.Map(IEndpointRouteBuilder builder)
            => builder
                .MapGet($"{Root.Endpoints.BASE_API_PATH}/todo/{{id}}", HandleAsync)

                .Produces<TodoItemOutput>()

                .Produces(StatusCodes.Status404NotFound);

        #endregion
    }
}