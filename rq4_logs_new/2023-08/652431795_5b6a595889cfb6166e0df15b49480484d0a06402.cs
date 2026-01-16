using Microsoft.AspNetCore.Mvc;
using Nameless.Microservice.Infrastructure;
using Nameless.Microservice.Web.Services;

namespace Nameless.Microservice.Web.Api.v1.Endpoints {
    public class Delete : IEndpoint {
        #region Public Static Methods

        public static IResult Handle([FromRoute] Guid id, TodoService todoService) {
            try {
                todoService.Delete(id);

                return Results.NoContent();
            }
            catch (ArgumentException ex) {
                return Results.Problem(ex.Message, statusCode: StatusCodes.Status400BadRequest);
            }
            catch (Exception ex) {
                return Results.Problem(ex.Message, statusCode: StatusCodes.Status500InternalServerError);
            }
        }

        #endregion

        #region IEndpoint Members

        public void Map(IEndpointRouteBuilder builder)
            => builder
                .MapDelete($"{Internals.Endpoints.BaseApiPath}/todo/{{id}}", Handle)

                .Produces(StatusCodes.Status204NoContent)
                .ProducesProblem(StatusCodes.Status400BadRequest)
                .ProducesProblem(StatusCodes.Status500InternalServerError)

                .WithOpenApi()

                .WithName(nameof(Delete))
                .WithDescription("Delete a To-Do")
                .WithSummary("Delete a To-Do")

                .WithApiVersionSet(builder.NewApiVersionSet("To-Do").Build())
                .HasApiVersion(1);

        #endregion
    }
}