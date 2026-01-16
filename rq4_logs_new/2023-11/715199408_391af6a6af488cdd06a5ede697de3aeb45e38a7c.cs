using NerdStore.Catalog.Domain.Interfaces.Repositories;
using NerdStore.Catalog.Domain.Interfaces.Services;

namespace NerdStore.Catalog.Domain.Services;

public class StockService : IStockService
{
    private readonly IProductRepository _productRepository;

    public StockService(IProductRepository productRepository)
    {
        _productRepository = productRepository;
    }

    public async Task<bool> DebitStockAsync(Guid productId, int quantity)
    {
        var product = await _productRepository.GetByIdAsync(productId);

        if (product is null || !product.HasStock(quantity)) return false;

        product.DebitStock(quantity);

        _productRepository.Update(product);

        return await _productRepository.UnitOfWork.Commit();
    }

    public async Task<bool> ReplenishStockAsync(Guid productId, int quantity)
    {
        var product = await _productRepository.GetByIdAsync(productId);

        if (product is null) return false;

        product.ReplenishStock(quantity);

        _productRepository.Update(product);

        return await _productRepository.UnitOfWork.Commit();
    }

    public void Dispose() => _productRepository.Dispose();
}