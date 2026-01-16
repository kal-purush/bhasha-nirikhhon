using MySqlX.XDevAPI;
using TiendaAPI.Common.DTO.Response;
using TiendaAPI.Data;
using TiendaAPI.Data.Entities;
using TiendaAPI.Data.Entities.Enums;
using TiendaAPI.Data.UoW;
using TiendaAPI.Services.Exceptions.NotFound;

namespace TiendaAPI.Services.Impl
{
    public class PedidoService : IPedidoService
    {
        private readonly IUnitOfWork _uow;
        private readonly CoreDbContext _dbContext;

        public PedidoService(IUnitOfWork uow, CoreDbContext dbContext)
        {
            _uow = uow ?? throw new ArgumentNullException(nameof(uow));
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
        }

        public async Task<PedidoResponse> CreatePedidoAsync(int ClienteId, EstadoPedidoEnum estado)
        {
            var Pedido = new Pedido
            {
                ClienteId = ClienteId,
                Estado = estado
            };

            _dbContext.Pedidos.Add(Pedido);
            await _dbContext.SaveChangesAsync();

            return new PedidoResponse
            {
                PedidoId = Pedido.PedidoId,
                ClienteId = Pedido.ClienteId,
                Estado = Pedido.Estado
            };
        }

        public async Task<bool> DeletePedido(int PedidoId)
        {
            var Pedido = await _uow.PedidoRepository.GetAsync(PedidoId);
            if (Pedido != null)
            {
                _uow.PedidoRepository.Delete(Pedido);
                await _uow.CommitAsync();
                return true;
            }
            return false;
        }

        public async Task<PedidoResponse> GetPedidoById(int PedidoId)
        {
            var Pedido = await _uow.PedidoRepository.GetAsync(PedidoId);
            var PedidoResponse = await mapPedido(Pedido);
            return PedidoResponse;
        }

        public async Task<List<PedidoResponse>> GetPedidoList()
        {
            List<PedidoResponse> PedidoResponses = new List<PedidoResponse>();
            var Pedidos = await _uow.PedidoRepository.GetAllAsync();
            foreach (var Pedido in Pedidos)
            {
                PedidoResponses.Add(await mapPedido(Pedido));
            }

            return PedidoResponses;
        }

        public async Task<PedidoResponse> UpdatePedido(int PedidoId, int ClienteId, EstadoPedidoEnum estado)
        {
            if (_uow.PedidoRepository.GetAsync(PedidoId) != null)
            {
                await _uow.PedidoRepository.UpdateAsync(new Pedido
                {
                    PedidoId = PedidoId,
                    ClienteId = ClienteId,
                    Estado = estado
                }, PedidoId);
                await _uow.CommitAsync();

                var Pedido = await _uow.PedidoRepository.GetAsync(PedidoId);
                return await mapPedido(Pedido);
            }
            else {
                throw new PedidoNotFoundException();
            }
            
        }

        public async Task<PedidoResponse> mapPedido(Pedido Pedido) {
            int PedidoIdDeseado = Pedido.PedidoId;
            var productosPedido = await _uow.ProductoPedidoRepository.FindAllAsync
                (
                productoPedido => productoPedido.PedidoId == PedidoIdDeseado
                );
            List<ProductoPedidoResponse> productosPedidoResponses = new List<ProductoPedidoResponse>();
            double total = 0;
            foreach (var productoPedido in productosPedido)
            {
                productosPedidoResponses.Add(new ProductoPedidoResponse
                {
                    PedidoId = productoPedido.PedidoId,
                    ProductoId = productoPedido.ProductoId,
                    ProductoPedidoId = productoPedido.ProductoPedidoId
                });
                total += productoPedido.Producto.Precio;
            }
            return new PedidoResponse
            {
                PedidoId = Pedido.PedidoId,
                ClienteId = Pedido.ClienteId,
                ProductosPedido = productosPedidoResponses,
                Total = total,
                Estado = Pedido.Estado
            };

        }
    }
}