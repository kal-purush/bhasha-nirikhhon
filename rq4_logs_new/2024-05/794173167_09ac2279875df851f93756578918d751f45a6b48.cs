using WebGeoInfrastructure.DTOs.Product;
using WebGeoInfrastructure.Helpers;
using WebGeoInfrastructure.Interfaces.Repositories;
using WebGeoInfrastructure.Interfaces.Services;

namespace WebGeo.BLL.Services
{
    public class ProductService : IProductService
    {
        private readonly IProductRepository _productRepository;
        public ProductService(IProductRepository productRepository)
        {
            _productRepository = productRepository;
        }
        public async Task<MessagingHelper> Create(ProductCreateDTO productCreate)
        {
            MessagingHelper response = new MessagingHelper();
            try
            {
                var create = await _productRepository.Create(productCreate.toEntity());
                if (!create)
                {
                    response.Success = false;
                    response.Message = "Cannot create this product";
                    return response;
                }
                response.Success = true;
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