using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using SPSS.Dto;
using SPSS.Entities;
using SPSS.Services;
using AutoMapper;
using System.IO;
using System.Threading.Tasks;
using SPSS.Dto.Request;
using SPSS.Dto.Response;
using SPSS.Services.FirebaseStorageService;
using SPSS.Services.ProductService;

namespace SPSS.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductsController(IFirebaseStorageService firebaseStorageService, IMapper mapper, IProductService productService) : ControllerBase
    {
        [HttpPost]
        public async Task<IActionResult> CreateProduct([FromForm] ProductRequest productRequest)
        {
            var product = mapper.Map<Product>(productRequest);

            if (productRequest.ImageFile != null)
            {
                using (var stream = productRequest.ImageFile.OpenReadStream())
                {
                    var fileName = $"{Guid.NewGuid()}_{productRequest.ImageFile.FileName}";
                    product.ImageUrl = await firebaseStorageService.UploadImageAsync(stream, fileName);
                }
            }

            await productService.AddAsync(product);

            var productResponse = mapper.Map<ProductResponse>(product);
            return Ok(productResponse);
        }

        [HttpGet("{id}")]
        public async Task<IActionResult> GetProduct(int id)
        {
            var product = await productService.GetByIdAsync(id);
            if (product == null)
            {
                return NotFound();
            }

            var productResponse = mapper.Map<ProductResponse>(product);
            return Ok(productResponse);
        }
    }
}