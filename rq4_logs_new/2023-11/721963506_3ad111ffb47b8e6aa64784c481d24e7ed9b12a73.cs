using TiendaAPI.Common.DTO.Response;

namespace TiendaAPI.Services
{
    public interface IProductoService
    {
        Task<ProductoResponse> CreateProductoAsync(string nombre, double precio, int stockDisponible);
        Task<List<ProductoResponse>> GetProductoList();
        Task<ProductoResponse> UpdateProducto(int ProductoId, string nombre, double precio, int stockDisponible);
        Task<bool> DeleteProducto(int ProductoId);
        Task<ProductoResponse> GetProductoById(int ProductoId);
    }
}