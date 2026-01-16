using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TondForooshApi.DTOs;
using TondForooshApi.Models;

namespace TondForooshApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductController : ControllerBase
    {
        private ITondForooshRepository repository;

        public ProductController(ITondForooshRepository repo)
        {
            repository = repo;
        }

        [HttpGet]
        public async Task<ActionResult<IEnumerable<Product>>> GetProducts()
        {
            var products = await repository.Products.ToListAsync();
            return Ok(products);
        }

        [HttpGet("{id}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<Product>> GetProduct(int id)
        {
            Product? p = await repository.Products.FirstOrDefaultAsync(p => p.Id == id);
            if (p == null)
                return NotFound();

            return Ok(p);
        }

        [HttpPost]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public async Task<ActionResult<int>> CreateNewProduct([FromBody] CreateProductDto createProductDto)
        {
            if (createProductDto == null)
            {
                return BadRequest();
            }

            var newProduct = new Product
            {
                Name = createProductDto.Name,
                Description = createProductDto.Description,
                Price = createProductDto.Price,
                ImageUrl = createProductDto.ImageUrl
            };

            var productId = await repository.AddAsync(newProduct);

            return Ok(productId);
        }
    }
}