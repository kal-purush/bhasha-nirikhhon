using TiendaAPI.Common.DTO.Response;
using TiendaAPI.Data;
using TiendaAPI.Data.Entities;
using TiendaAPI.Data.UoW;
using TiendaAPI.Services.Exceptions.NotFound;

namespace TiendaAPI.Services.Impl
{
    public class ProductoPedidoService : IProductoPedidoService
    {
        private readonly IUnitOfWork _uow;
        private readonly CoreDbContext _dbContext;

        public ProductoPedidoService(IUnitOfWork uow, CoreDbContext dbContext)
        {
            _uow = uow ?? throw new ArgumentNullException(nameof(uow));
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
        }

        public async Task<ProductoPedidoResponse> CreateProductoPedidoAsync(int ProductoId, int PedidoId)
        {
            var ProductoPedido = new ProductoPedido
            {
                ProductoId = ProductoId,
                PedidoId = PedidoId
            };

            _dbContext.ProductoPedidos.Add(ProductoPedido);
            await _dbContext.SaveChangesAsync();

            var Pedido = await _uow.PedidoRepository.GetAsync(PedidoId);
            var Producto = await _uow.ProductoRepository.GetAsync(ProductoId);

            Pedido.Total += Producto.Precio;
            await _uow.PedidoRepository.UpdateAsync(Pedido, PedidoId);
            await _dbContext.SaveChangesAsync();

            return new ProductoPedidoResponse
            {
                ProductoPedidoId = ProductoPedido.ProductoPedidoId,
                ProductoId = ProductoPedido.ProductoId,
                PedidoId = ProductoPedido.PedidoId
            };
        }

        public async Task<bool> DeleteProductoPedido(int ProductoPedidoId)
        {
            var ProductoPedido = await _uow.ProductoPedidoRepository.GetAsync(ProductoPedidoId);
            if (ProductoPedido != null)
            {
                _uow.ProductoPedidoRepository.Delete(ProductoPedido);
                await _uow.CommitAsync();

                var Pedido = await _uow.PedidoRepository.GetAsync(ProductoPedido.PedidoId);
                var Producto = await _uow.ProductoRepository.GetAsync(ProductoPedido.ProductoId);
                Pedido.Total -= Producto.Precio;
                await _uow.PedidoRepository.UpdateAsync(Pedido, ProductoPedido.PedidoId);
                await _dbContext.SaveChangesAsync();

                return true;
            }
            return false;
        }

        public async Task<ProductoPedidoResponse> GetProductoPedidoById(int ProductoPedidoId)
        {
            var ProductoPedido = await _uow.ProductoPedidoRepository.GetAsync(ProductoPedidoId);
            var ProductoPedidoResponse = await mapProductoPedido(ProductoPedido);
            return ProductoPedidoResponse;
        }

        public async Task<List<ProductoPedidoResponse>> GetProductoPedidoList()
        {
            List<ProductoPedidoResponse> ProductoPedidoResponses = new List<ProductoPedidoResponse>();
            var ProductoPedidos = await _uow.ProductoPedidoRepository.GetAllAsync();
            foreach (var ProductoPedido in ProductoPedidos)
            {
                ProductoPedidoResponses.Add(await mapProductoPedido(ProductoPedido));
            }

            return ProductoPedidoResponses;
        }

        
        public async Task<ProductoPedidoResponse> mapProductoPedido(ProductoPedido ProductoPedido) {
            //pedidos
            int ProductoPedidoIdDeseado = ProductoPedido.ProductoPedidoId;            
            return new ProductoPedidoResponse
            {
                ProductoPedidoId = ProductoPedido.ProductoPedidoId,
                ProductoId = ProductoPedido.ProductoId,
                PedidoId = ProductoPedido.PedidoId
            };
        }
    }
}