using OnlineStore.Domain.Entities;
using OnlineStore.Service.DTOs.Product;

namespace OnlineStore.Service.Interfaces
{
    public interface IProductService
    {
        Task<Product> AddProductAsync(ProductForCreationDto productDto);
        Task<IQueryable<Product>> RetrieveAllProducts();
        Task<Product> RetrieveProductByIdAsync(int productId);
        Task<Product> ModifyProductAsync(int productId, ProductForUpdateDTo productDto);
        Task<bool> RemoveProductByIdAsync(int productId);
    }
}