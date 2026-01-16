using dotnet9_ketnoigiaothuong.Infrastructure.Exceptions;
using dotnet9_ketnoigiaothuong.Infrastructure.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Security.Claims;
using static dotnet9_ketnoigiaothuong.Domain.Contracts.ProductContract;

namespace dotnet9_ketnoigiaothuong.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductController : BaseController
    {
        [HttpGet]
        public async Task<ActionResult<IEnumerable<ResponseProductModel>>> GetAllProducts()
        {
            try
            {
                var products = await Provider.ProductService.GetAllProductsAsync();
                return Ok(products);
            }
            catch (Exception ex)
            {
                throw new ApiException(ex.Message);
            }
        }

        [HttpGet("{id}")]
        public async Task<ActionResult<ProductDetailModel>> GetProductById(int id)
        {
            try
            {
                var product = await Provider.ProductService.GetProductByIdAsync(id);
                if (product == null)
                    return NotFound("Không tìm thấy sản phẩm với ID đã cho");

                return Ok(product);
            }
            catch (Exception ex)
            {
                throw new ApiException(ex.Message);
            }
        }

        [HttpPost]
        [Authorize]
        public async Task<ActionResult<ApiResponse<ResponseProductModel>>> CreateProduct([FromBody] CreateProductModel model)
        {
            try
            {
                var userIdClaim = User.FindFirst(ClaimTypes.NameIdentifier);
                if (userIdClaim == null)
                    return Unauthorized(new ApiResponse<ResponseProductModel>(false, "Không tìm thấy thông tin người dùng", null));

                if (!int.TryParse(userIdClaim.Value, out int userId))
                    return BadRequest(new ApiResponse<ResponseProductModel>(false, "UserID không hợp lệ", null));

                var result = await Provider.ProductService.CreateProductAsync(model, userId);
                
                if (!result.Success)
                    return BadRequest(result);

                return Ok(result);
            }
            catch (Exception ex)
            {
                throw new ApiException(ex.Message);
            }
        }

        [HttpPut("{id}")]
        [Authorize] 
        public async Task<ActionResult<ApiResponse<ResponseProductModel>>> UpdateProduct(int id, [FromBody] UpdateProductModel model)
        {
            try
            {
                var result = await Provider.ProductService.UpdateProductAsync(id, model);
                
                if (!result.Success)
                    return BadRequest(result);

                return Ok(result);
            }
            catch (Exception ex)
            {
                throw new ApiException(ex.Message);
            }
        }

        [HttpDelete("{id}")]
        [Authorize]
        public async Task<ActionResult<ApiResponse<bool>>> DeleteProduct(int id)
        {
            try
            {
                var result = await Provider.ProductService.DeleteProductAsync(id);
                
                if (!result.Success)
                    return BadRequest(result);

                return Ok(result);
            }
            catch (Exception ex)
            {
                throw new ApiException(ex.Message);
            }
        }
    }
} 