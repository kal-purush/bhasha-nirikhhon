using API2.Data;
using API2.Entities;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Runtime.CompilerServices;

namespace API2.Controllers
{
    [ApiController]
    [Route("/[controller]")]
    public class ProductsController : Controller
    {
        private readonly StoreContext _storeContext;
        public ProductsController(StoreContext storeContext)
        {
            this._storeContext = storeContext;
        }
        [HttpGet]
        [Route("/[action]")]
        public async Task<ActionResult<List<Product>>> GetProducts()
        {
            return await _storeContext.Products.ToListAsync();
        }
        [HttpGet]
        [Route("/[action]")]
        public async Task<ActionResult<Product>> GetProduct(int id)
        {
            return await _storeContext.Products.FindAsync(id);

        }
    }
}