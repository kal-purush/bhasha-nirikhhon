using AutoMapper;
using iPhoneBE.Data;
using iPhoneBE.Data.Model;
using iPhoneBE.Data.Models.ProductItemModel;
using iPhoneBE.Data.Models.ProductModel;
using iPhoneBE.Data.ViewModels.ProductDTO;
using iPhoneBE.Data.ViewModels.ProductItemDTO;
using iPhoneBE.Service.Interfaces;
using iPhoneBE.Service.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace iPhoneBE.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductItemController : ControllerBase
    {
        private readonly IProductItemServices _productItemServices;
        private readonly IMapper mapper;
        private readonly IUnitOfWork _unitOfWork;

        public ProductItemController(IProductItemServices productItemServices, IMapper mapper, IUnitOfWork unitOfWork)
        {
            _productItemServices = productItemServices;
            this.mapper = mapper;
            _unitOfWork = unitOfWork;
        }

        [HttpGet]
        //[Authorize(Roles = "Admin, Staff, Customer")]
        public async Task<ActionResult<IEnumerable<ProductItemViewModel>>> GetAll(string? productItemName = null)
        {
            var productItems = await _productItemServices.GetAllAsync(productItemName);
            return Ok(mapper.Map<List<ProductItemViewModel>>(productItems));
        }

        [HttpGet("{id}")]
        //[Authorize(Roles = "Admin, Staff, Customer")]
        public async Task<ActionResult<ProductItem>> GetById(int id)
        {
            var productItem = await _productItemServices.GetByIdAsync(id);
            if (productItem == null)
                return NotFound();

            return Ok(mapper.Map<ProductItemViewModel>(productItem));
        }

        [HttpPost("create-product")]
        //[Authorize(Roles = "Admin, Staff")]
        public async Task<ActionResult<ProductItem>> Add([FromBody] CreateProductItemModel CreateProductItem)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            _unitOfWork.BeginTransaction();
            try
            {
                var product = mapper.Map<ProductItem>(CreateProductItem);
                if (product == null)
                {
                    return BadRequest("Invalid product data!");
                }
                product = await _productItemServices.AddAsync(product);

                _unitOfWork.SaveChanges();
                _unitOfWork.CommitTransaction();

                return Ok(mapper.Map<ProductItemViewModel>(product));
            }
            catch (ArgumentNullException ex)
            {
                _unitOfWork.RollbackTransaction();
                return BadRequest(new { message = ex.Message });
            }
            catch (ArgumentException ex)
            {
                _unitOfWork.RollbackTransaction();
                return BadRequest(new { message = ex.Message });
            }
            catch (KeyNotFoundException ex)
            {
                _unitOfWork.RollbackTransaction();
                return NotFound(new { message = ex.Message });
            }
            catch (Exception ex)
            {
                _unitOfWork.RollbackTransaction();
                return StatusCode(StatusCodes.Status500InternalServerError, new { message = ex.Message });
            }
        }


        [HttpPut("{id}")]
        //[Authorize(Roles = "Admin")]
        public async Task<IActionResult> Update(int id, [FromBody] UpdateProductItemModel updateProduct)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            _unitOfWork.BeginTransaction();
            try
            {
                var existingProductItem = await _productItemServices.GetByIdAsync(id);
                if (existingProductItem == null)
                {
                    return NotFound($"Product Item with ID {id} not found.");
                }

                var product = mapper.Map<ProductItem>(updateProduct);
                product = await _productItemServices.UpdateAsync(id, product);

                _unitOfWork.SaveChanges();
                _unitOfWork.CommitTransaction();

                return Ok(mapper.Map<ProductItemViewModel>(product));
            }
            catch (ArgumentNullException ex)
            {
                _unitOfWork.RollbackTransaction();
                return NoContent();
            }
            catch (KeyNotFoundException ex)
            {
                _unitOfWork.RollbackTransaction();
                return NotFound(new { message = ex.Message });
            }
            catch (Exception ex)
            {
                _unitOfWork.RollbackTransaction();
                return StatusCode(StatusCodes.Status500InternalServerError, new { message = ex.Message });
            }
        }

        [HttpDelete("{id}")]
        //[Authorize(Roles = "Admin")]
        public async Task<IActionResult> DeleteProduct(int id)
        {
            _unitOfWork.BeginTransaction();
            try
            {
                var existingProductItem = await _productItemServices.GetByIdAsync(id);
                if (existingProductItem == null)
                {
                    return NotFound($"Product Item with ID {id} not found.");
                }

                await _productItemServices.DeleteAsync(id);

                _unitOfWork.SaveChanges();
                _unitOfWork.CommitTransaction();

                return Ok(mapper.Map<ProductItemViewModel>(existingProductItem));
            }
            catch (KeyNotFoundException ex)
            {
                _unitOfWork.RollbackTransaction();
                return NotFound(new { message = ex.Message });
            }
            catch (Exception ex)
            {
                _unitOfWork.RollbackTransaction();
                return StatusCode(StatusCodes.Status500InternalServerError, new { message = ex.Message });
            }
        }
    }
}