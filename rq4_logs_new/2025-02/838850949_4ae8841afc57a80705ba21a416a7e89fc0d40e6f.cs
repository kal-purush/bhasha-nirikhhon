using ModelLibrary.Dto;
using Web.Models;
using Web.Service.IService;
using Web.Utility;

namespace Web.Service
{
    public class CartService : ICartService
    {
        private readonly IBaseService _baseService;

        public CartService(IBaseService baseService)
        {
            _baseService = baseService;
        }

        public async Task<ResponseDto?> GetCart(string userId)
        {
            return await _baseService.SendAsync(new RequestDto()
            {
                ApiType = StaticDetails.ApiType.GET,
                Url = StaticDetails.CartApiBase + "/api/cart",
                Data = userId
            });
        }

        public async Task<ResponseDto?> UpsetrCart(CartDto cartDto)
        {
            return await _baseService.SendAsync(new RequestDto()
            {
                ApiType = StaticDetails.ApiType.POST,
                Url = StaticDetails.CartApiBase + "/api/cart",
                Data = cartDto
            });
        }

        public async Task<ResponseDto?> UpsetrCoupon(string userId, string couponCode)
        {
            return await _baseService.SendAsync(new RequestDto()
            {
                ApiType = StaticDetails.ApiType.PUT,
                Url = StaticDetails.CartApiBase + "/api/cart/"+ couponCode,
                Data = userId
            });
        }

        public async Task<ResponseDto?> DeleteCart(string userId)
        {
            return await _baseService.SendAsync(new RequestDto()
            {
                ApiType = StaticDetails.ApiType.DELETE,
                Url = StaticDetails.CartApiBase + "/api/cart/deleteall",
                Data = userId
            });
        }

        public async Task<ResponseDto?> DeleteDetail(int detailId)
        {
            return await _baseService.SendAsync(new RequestDto()
            {
                ApiType = StaticDetails.ApiType.DELETE,
                Url = StaticDetails.CartApiBase + "/api/cart",
                Data = detailId
            });
        }
    }
}