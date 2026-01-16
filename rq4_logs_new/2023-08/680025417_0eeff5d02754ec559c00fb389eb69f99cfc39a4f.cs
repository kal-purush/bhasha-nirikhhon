using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using ShopOnline.Api.Repositories.Contracts;
using ShopOnline.Models.Dtos;
using ShopOnline.Api.Extentions;
using ShopOnline.Api.Entities;


namespace ShopOnline.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ShopingCartController : ControllerBase
    {
        private readonly IShopingCartReopository _shopingCartReopository;
        private readonly IProductRepostiories _productRepostiories;

        public ShopingCartController(IShopingCartReopository shopingCartReopository,
                                    IProductRepostiories productRepostiories)
        {
            _shopingCartReopository = shopingCartReopository;
            _productRepostiories = productRepostiories;
        }

        [HttpGet]
        [Route("{userId}/GetItems")]
        public async Task<ActionResult<IEnumerable<CartItemDto>>> GetItems(int userId) 
        {
            try
            {
                var cartItems = await _shopingCartReopository.GetItems(userId);
                if(cartItems == null)
                {
                    return NoContent();
                }
                var products = await _productRepostiories.GetItems();
                // Throw an exception to web component that no products in the database
                if(products == null)
                {
                    throw new Exception("No products exist in the system");
                }
                var cartItemDto = cartItems.ConvertToDto(products);
                return Ok(cartItemDto);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
            
        }

        [HttpGet("{id:int}")]
        public async Task<ActionResult<CartItemDto>> GetItem(int id)
        {
            try
            {
                var cartItem = await _shopingCartReopository.GetItem(id);
                if(cartItem == null)
                {
                    return NotFound();
                }
                var product = await _productRepostiories.GetItem(cartItem.ProductId);
                if(product == null)
                {
                    throw new Exception("No proudct exit in the system");
                }
                var cartItemDto = cartItem.ConvertToDto(product);
                return Ok(cartItemDto);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }

        [HttpPost]
        public async Task<ActionResult<CartItemDto>> PostItem([FromBody] CartItemToAddDto cartItemToAddDto)
        {
            try
            {
                var newCartItem = await _shopingCartReopository.AddItem(cartItemToAddDto);
                if(newCartItem == null)
                {
                    return NotFound();
                }
                var product = await _productRepostiories.GetItem(newCartItem.ProductId);
                if (product == null)
                {
                    throw new Exception($"Something went wrong when attemping to retrieve product (productId:({cartItemToAddDto.ProductId}");
                }

                var newCartItemDto = newCartItem.ConvertToDto(product);
                // standerd action to post method to return the location
                return CreatedAtAction(nameof(GetItem), new { id = newCartItemDto.Id }, newCartItemDto);
            }
            catch (Exception ex)
            {

                return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
            }
        }


    }

}