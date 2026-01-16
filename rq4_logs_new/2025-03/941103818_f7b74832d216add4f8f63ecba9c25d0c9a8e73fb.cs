using Goal.Application.Interfaces.Product;
using Goal.Shared.Models.Response.Product;
using MediatR;
using Microsoft.Extensions.Logging;

namespace Goal.Application.Activities.Product;

public sealed record UpdateProductDescriptionActivity(int ProductId, string NewDescription) : IRequest<ProductResponse?>
{
    public sealed class Handler(IProductService service, ILogger<UpdateProductDescriptionActivity> logger) : IRequestHandler<UpdateProductDescriptionActivity, ProductResponse?>
    {
        public async Task<ProductResponse?> Handle(UpdateProductDescriptionActivity request, CancellationToken cancellationToken)
        {
            var result = await service.UpdateProductDescriptionAsync(request.ProductId, request.NewDescription);
            if (result == null)
            {
                logger.LogWarning("Product with ID {ProductId} not found", request.ProductId);
            }
            return result;
        }
    }
}