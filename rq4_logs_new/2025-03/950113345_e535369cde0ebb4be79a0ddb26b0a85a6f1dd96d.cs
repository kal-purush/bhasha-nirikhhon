using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using Project.Data;
using Project.Models;
using Project.Repository;
using System.Net;
using Microsoft.AspNetCore.Http;
using Microsoft.EntityFrameworkCore;



namespace Project.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CartController : ControllerBase
    {


        private readonly ILogger<CartController> _logger;
        private readonly ProjectDbContext _dbContext;
        private IShopingCartRepository _cartRepository;
        private readonly IMapper _mapper;
        private APIResponse _apiResponse;



        public CartController(ILogger<CartController> logger, ProjectDbContext dbContext, IMapper mapper, IShopingCartRepository cartRepository)
        {
            _logger = logger;
            _dbContext = dbContext;
            _mapper = mapper;
            _apiResponse = new APIResponse();
            _cartRepository = cartRepository;
        }

        [HttpGet("All", Name = "GetAllProducts")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]


        public async Task<ActionResult<APIResponse>> GetAllProductsAsync()
        {
            try
            {
                _logger.LogInformation("GetAllProducts started working");

                List<ShopingCart> cart = await _cartRepository.GetAllAsync();

                _apiResponse.Data = _mapper.Map<List<ShopingCartDTO>>(cart);
                _apiResponse.StatusCode = HttpStatusCode.OK;
                _apiResponse.status = true;

                return Ok(_apiResponse);
            }
            catch (Exception ex)
            {
                _apiResponse.status = false;
                _apiResponse.StatusCode = HttpStatusCode.InternalServerError;

                return _apiResponse;
            }
        }

        [HttpPost("Create")]
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<ActionResult<APIResponse>> CreateProductInCartAsync([FromBody] ShopingCartDTO dto)
        {

            try
            {
                _logger.LogInformation("CreateProductInCart started Working");

                if (dto == null)
                    return BadRequest();

                ShopingCart cart = _mapper.Map<ShopingCart>(dto);

                ShopingCart cartAfterCreation = await _cartRepository.CreateAsync(cart);

                dto.Id = cartAfterCreation.Id;

                _apiResponse.Data = dto;
                _apiResponse.status = true;
                _apiResponse.StatusCode = HttpStatusCode.OK;

                return Ok(_apiResponse.Data);
            }
            catch (Exception ex)
            {
                _apiResponse.status = false;
                _apiResponse.StatusCode = HttpStatusCode.InternalServerError;

                return _apiResponse;
            }
        }



        [HttpDelete("{id:int}", Name = "DeleteProductInCart")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]

        public async Task<ActionResult<APIResponse>> DeleteProductInCartAsync(int id)
        {
            try
            {
                _logger.LogInformation("DeleteProductInCart started Working");

                if (id <= 0)
                    return BadRequest();

                ShopingCart cart = await _cartRepository.GetByAsync(x => x.Id == id);

                if (cart == null)
                    return NotFound();

                await _cartRepository.DeleteAsync(cart);

                _apiResponse.Data = true;
                _apiResponse.StatusCode = HttpStatusCode.OK;
                _apiResponse.status = true;

                return Ok(_apiResponse);
            }
            catch (Exception ex)
            {
                _apiResponse.status = false;
                _apiResponse.StatusCode = HttpStatusCode.InternalServerError;

                return _apiResponse;
            }
        }

        //getTotalPrice
    }
}