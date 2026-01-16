using MediatR;
using NerdStore.Catalog.Domain.Interfaces.Repositories;

namespace NerdStore.Catalog.Domain.Events.Handlers;

public class ProductEventHandler : INotificationHandler<ProductBelowStockEvent>
{
    private readonly IProductRepository _productRepository;

    public ProductEventHandler(IProductRepository productRepository)
    {
        _productRepository = productRepository;
    }

    public async Task Handle(ProductBelowStockEvent message, CancellationToken cancellationToken)
    {
        var product = await _productRepository.GetByIdAsync(message.AggregateId);

        // Send an email for acquisition for more products
    }
}