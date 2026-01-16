using AutoMapper;
using iPhoneBE.Data.Entities;
using iPhoneBE.Data.Models.ProductItemAttributeModel;
using iPhoneBE.Data.ViewModels.ProductItemAttributeDTO;
using iPhoneBE.Service.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace iPhoneBE.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductItemAttributeController : ControllerBase
    {
        private readonly IProductItemAttributeServices _productItemAttributeServices;
        private readonly IMapper _mapper;

        public ProductItemAttributeController(IProductItemAttributeServices productItemAttributeServices, IMapper mapper)
        {
            _productItemAttributeServices = productItemAttributeServices;
            _mapper = mapper;
        }

        [HttpGet]
        public async Task<ActionResult<IEnumerable<ProductItemAttributeViewModel>>> GetAll()
        {
            try
            {
                var productItemAttributes = await _productItemAttributeServices.GetAllAsync();
                return Ok(_mapper.Map<List<ProductItemAttributeViewModel>>(productItemAttributes));
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError,
                    new { message = ex.Message, details = ex.StackTrace });
            }
        }

        [HttpGet("{id}")]
        public async Task<ActionResult<ProductItemAttributeViewModel>> GetById(int id)
        {
            var productItemAttribute = await _productItemAttributeServices.GetByIdAsync(id);
            return Ok(_mapper.Map<ProductItemAttributeViewModel>(productItemAttribute));
        }

        [HttpPost]
        public async Task<ActionResult<ProductItemAttributeViewModel>> Add([FromBody] CreateProductItemAttributeModel createProductItemAttribute)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            var productItemAttribute = _mapper.Map<ProductItemAttribute>(createProductItemAttribute);

            if (productItemAttribute == null)
            {
                return BadRequest("Invalid product item attribute data.");
            }

            productItemAttribute = await _productItemAttributeServices.AddAsync(productItemAttribute);

            return CreatedAtAction(nameof(GetById),
                new { id = productItemAttribute.ProductItemAttributeID },
                _mapper.Map<ProductItemAttributeViewModel>(productItemAttribute));
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> Update(int id, [FromBody] UpdateProductItemAttributeModel updateProductItemAttribute)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            var productItemAttribute = await _productItemAttributeServices.UpdateAsync(id, updateProductItemAttribute);

            return Ok(_mapper.Map<ProductItemAttributeViewModel>(productItemAttribute));
        }

        [HttpDelete("{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            var deletedProductItemAttribute = await _productItemAttributeServices.DeleteAsync(id);

            return Ok(_mapper.Map<ProductItemAttributeViewModel>(deletedProductItemAttribute));
        }
    }
}