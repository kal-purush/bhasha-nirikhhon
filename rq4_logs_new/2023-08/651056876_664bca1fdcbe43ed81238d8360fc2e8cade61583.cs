using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using Vastra.API.Models;
using Vastra.API.Services;

namespace Vastra.API.Controllers
{
    [ApiController]
    [Route("api/roles/{roleId}/users/{userId}/orders/{orderId}/cartItems")]
    public class CartItemsController : Controller
    {
        private readonly IVastraRepository _vastraRepository;
        private readonly IMapper _mapper;
        private const int maxCartItemPageSize = 10;
        public CartItemsController(IVastraRepository vastraRepository, IMapper mapper)
        {
            _vastraRepository = vastraRepository;
            _mapper = mapper;
        }
        [HttpGet]
        public async Task<ActionResult<IEnumerable<CartItemDto>>> GetCartItems(int roleId, int userId, int orderId, int pageNumber= 1, int pageSize = 10)
        {
            if(!await _vastraRepository.RoleExistsAsync(roleId))
            {
                return NotFound();
            }
            if(!await _vastraRepository.UserExistsAsync(userId))
            {
                return NotFound();
            }
            if(!await _vastraRepository.OrderExistsForUser(userId, orderId))
            {
                return NotFound();
            }
            if(pageSize > maxCartItemPageSize)
            {
                pageSize = maxCartItemPageSize;
            }
            var (cartItemEntities, paginationMetadata) = await _vastraRepository.GetCartItemsForOrderAsync(orderId, pageNumber, pageSize);
            
            return Ok(_mapper.Map<CartItemDto>(cartItemEntities));
        }
    }
}