using AutoMapper;
using iPhoneBE.Data;
using iPhoneBE.Data.Model;
using iPhoneBE.Data.Models.ProductModel;
using iPhoneBE.Data.ViewModels.ProductDTO;
using iPhoneBE.Service.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace iPhoneBE.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductController : ControllerBase
    {
        private readonly IProductServices _productServices;
        private readonly IMapper mapper;
        private readonly IUnitOfWork _unitOfWork;

        public ProductController(IProductServices productServices, IMapper mapper, IUnitOfWork unitOfWork)
        {
            _productServices = productServices;
            this.mapper = mapper;
            _unitOfWork = unitOfWork;
        }

        [HttpGet]
        //[Authorize(Roles = "Admin, Staff, Customer")]
        public async Task<ActionResult<IEnumerable<ProductViewModel>>> GetAll(string? productName = null)
        {
            var categories = await _productServices.GetAllAsync(productName);
            return Ok(mapper.Map<List<ProductViewModel>>(categories));
        }

        [HttpGet("{id}")]
        //[Authorize(Roles = "Admin, Staff, Customer")]
        public async Task<ActionResult<Product>> GetById(int id)
        {
            var product = await _productServices.GetByIdAsync(id);
            if (product == null)
                return NotFound();

            return Ok(mapper.Map<ProductViewModel>(product));
        }

        [HttpPost("create-product")]
        //[Authorize(Roles = "Admin, Staff")]
        public async Task<ActionResult<Product>> Add([FromBody] CreateProductModel CreateProduct)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            _unitOfWork.BeginTransaction();
            try
            {
                var product = mapper.Map<Product>(CreateProduct);
                if (product == null)
                {
                    return BadRequest("Invalid product data!");
                }
                product = await _productServices.AddAsync(product);

                _unitOfWork.SaveChanges();
                _unitOfWork.CommitTransaction();

                return Ok(mapper.Map<ProductViewModel>(product));
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
        public async Task<IActionResult> Update(int id, [FromBody] UpdateProductModel updateProduct)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            _unitOfWork.BeginTransaction();
            try
            {
                var existingProduct = await _productServices.GetByIdAsync(id);
                if (existingProduct == null)
                {
                    return NotFound($"Product with ID {id} not found.");
                }

                var product = mapper.Map<Product>(updateProduct);
                product = await _productServices.UpdateAsync(id, product);

                _unitOfWork.SaveChanges();
                _unitOfWork.CommitTransaction();

                return Ok(mapper.Map<ProductViewModel>(product));
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
                var existingProduct = await _productServices.GetByIdAsync(id);
                if (existingProduct == null)
                {
                    return NotFound($"Product with ID {id} not found.");
                }

                await _productServices.DeleteAsync(id);

                _unitOfWork.SaveChanges();
                _unitOfWork.CommitTransaction();

                return Ok(mapper.Map<ProductViewModel>(existingProduct));
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