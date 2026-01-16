using AutoMapper;
using iPhoneBE.Data.Model;
using iPhoneBE.Data.Models.ProductItemModel;
using iPhoneBE.Data.ViewModels.ProductItemDTO;
using iPhoneBE.Service.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace iPhoneBE.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductItemController : ControllerBase
    {
        private readonly IProductItemServices _productItemServices;
        private readonly IMapper _mapper;

        public ProductItemController(IProductItemServices productItemServices, IMapper mapper)
        {
            _productItemServices = productItemServices;
            _mapper = mapper;
        }

        [HttpGet]
        public async Task<ActionResult<IEnumerable<ProductItemViewModel>>> GetAll()
        {
            try
            {
                var productItems = await _productItemServices.GetAllAsync();
                return Ok(_mapper.Map<List<ProductItemViewModel>>(productItems));
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError,
                    new { message = ex.Message, details = ex.StackTrace });
            }
        }

        [HttpGet("{id}")]
        public async Task<ActionResult<ProductItemViewModel>> GetById(int id)
        {
            var productItem = await _productItemServices.GetByIdAsync(id);
            return Ok(_mapper.Map<ProductItemViewModel>(productItem));
        }

        [HttpPost]
        public async Task<ActionResult<ProductItemViewModel>> Add([FromBody] CreateProductItemModel createProductItem)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            var productItem = _mapper.Map<ProductItem>(createProductItem);

            if (productItem == null)
            {
                return BadRequest("Invalid product item data.");
            }

            productItem = await _productItemServices.AddAsync(productItem);

            var result = _mapper.Map<ProductItemViewModel>(productItem);
            return CreatedAtAction(nameof(GetById),
                new { id = productItem.ProductItemID },
                result);
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> Update(int id, [FromBody] UpdateProductItemModel updateProductItem)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            var productItem = await _productItemServices.UpdateAsync(id, updateProductItem);

            return Ok(_mapper.Map<ProductItemViewModel>(productItem));
        }

        [HttpDelete("{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            var deletedProductItem = await _productItemServices.DeleteAsync(id);

            return Ok(_mapper.Map<ProductItemViewModel>(deletedProductItem));
        }
    }
}