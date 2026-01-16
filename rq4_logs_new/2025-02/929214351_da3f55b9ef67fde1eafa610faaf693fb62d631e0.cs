using AutoMapper;
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
        private readonly IMapper _mapper;

        public ProductController(IProductServices productServices, IMapper mapper)
        {
            _productServices = productServices;
            _mapper = mapper;
        }

        [HttpGet]
        public async Task<ActionResult<IEnumerable<ProductViewModel>>> GetAll()
        {
            try
            {
                var products = await _productServices.GetAllAsync();
                return Ok(_mapper.Map<List<ProductViewModel>>(products));
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError,
                    new { message = ex.Message, details = ex.StackTrace });
            }
        }

        [HttpGet("{id}")]
        public async Task<ActionResult<ProductViewModel>> GetById(int id)
        {
            var product = await _productServices.GetByIdAsync(id);
            return Ok(_mapper.Map<ProductViewModel>(product));
        }

        [HttpPost]
        public async Task<ActionResult<ProductViewModel>> Add([FromBody] CreateProductModel createProduct)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            var product = _mapper.Map<Product>(createProduct);

            if (product == null)
            {
                return BadRequest("Invalid product data.");
            }

            product = await _productServices.AddAsync(product);

            return CreatedAtAction(nameof(GetById),
                new { id = product.ProductID },
                _mapper.Map<ProductViewModel>(product));
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> Update(int id, [FromBody] UpdateProductModel updateProduct)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            var product = await _productServices.UpdateAsync(id, updateProduct);

            return Ok(_mapper.Map<ProductViewModel>(product));
        }

        [HttpDelete("{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            var deletedProduct = await _productServices.DeleteAsync(id);

            return Ok(_mapper.Map<ProductViewModel>(deletedProduct));
        }
    }
}