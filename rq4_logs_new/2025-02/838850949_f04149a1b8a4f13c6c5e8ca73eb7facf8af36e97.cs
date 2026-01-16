using ModelLibrary.Dto;
using Web.Models;
using Web.Service.IService;
using Web.Utility;

namespace Web.Service
{
	public class ProductService : IProductService
	{
		private readonly IBaseService _baseService;

		public ProductService(IBaseService baseService)
		{
			_baseService = baseService;
		}

		public async Task<ResponseDto?> GetAllProductsAsync()
		{
			return await _baseService.SendAsync(new RequestDto()
			{
				ApiType = StaticDetails.ApiType.GET,
				Url = StaticDetails.CouponApiBase + "/api/product"
			});
		}

		public async Task<ResponseDto?> GetProductByIdAsync(int id)
		{
			return await _baseService.SendAsync(new RequestDto()
			{
				ApiType = StaticDetails.ApiType.GET,
				Url = StaticDetails.CouponApiBase + "/api/product/" + id
			});
		}

		public async Task<ResponseDto?> CreateProductAsync(ProductDto dto)
		{
			return await _baseService.SendAsync(new RequestDto()
			{
				ApiType = StaticDetails.ApiType.POST,
				Url = StaticDetails.CouponApiBase + "/api/product",
				Data = dto
			});
		}

		public async Task<ResponseDto?> UpdateProductAsync(ProductDto dto)
		{
			return await _baseService.SendAsync(new RequestDto()
			{
				ApiType = StaticDetails.ApiType.PUT,
				Url = StaticDetails.CouponApiBase + "/api/product",
				Data = dto
			});
		}

		public async Task<ResponseDto?> DeleteProductAsync(int id)
		{
			return await _baseService.SendAsync(new RequestDto()
			{
				ApiType = StaticDetails.ApiType.DELETE,
				Url = StaticDetails.CouponApiBase + "/api/product/" + id,
			});
		}
	}
}