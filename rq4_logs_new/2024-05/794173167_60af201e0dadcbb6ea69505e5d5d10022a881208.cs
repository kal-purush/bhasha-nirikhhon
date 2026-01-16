using WebGeoInfrastructure.DTOs.Shop;
using WebGeoInfrastructure.Helpers;
using WebGeoInfrastructure.Interfaces.Repositories;
using WebGeoInfrastructure.Interfaces.Services;

namespace WebGeo.BLL.Services
{
    public class ShopService : IShopService
    {
        private readonly IShopRepository _shopRepository;
        public ShopService(IShopRepository shopRepository)
        {
            _shopRepository = shopRepository;
        }

        public async Task<MessagingHelper> Create(CreateShopDTO createShop)
        {
            MessagingHelper response = new MessagingHelper();
            try
            {
                var create = await _shopRepository.Create(createShop.toEntity());
                if (!create)
                {
                    response.Success = false;
                    response.Message = "Cannot insert this Shop";
                    return response;
                }

                response.Success = create;
            }
            catch (Exception ex)
            {
                response.Success = false;
                response.Message = ex.Message;
            }
            return response;
        }
    }
}