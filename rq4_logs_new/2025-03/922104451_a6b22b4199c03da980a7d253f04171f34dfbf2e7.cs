namespace Microstore.Service.OrderingApi.Endpoints;

public record CreateOrderRequest(OrderDto Order);
public record CreateOrderResponse(Guid Id);

public class CreateOrder : ICarterModule
{
    public void AddRoutes(IEndpointRouteBuilder app)
    {
        app.MapPost("/orders", async (CreateOrderRequest request, ISender sender) =>
        {
            CreateOrderCommand command = request.Adapt<CreateOrderCommand>();
            CreateOrderResult result = await sender.Send(command);
            CreateOrderResponse response = result.Adapt<CreateOrderResponse>();
            return Results.Created($"/orders/{response.Id}", response);
        })
        .WithName("CreateOrder")
        .Produces<CreateOrderResponse>(StatusCodes.Status201Created)
        .ProducesProblem(StatusCodes.Status400BadRequest)
        .WithSummary("Create order")
        .WithDescription("Create order");
    }
}