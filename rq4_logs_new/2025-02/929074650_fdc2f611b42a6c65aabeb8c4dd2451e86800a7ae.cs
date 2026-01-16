using System;
using Core.Entities;
using Infrastructure.Data;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace API.Controllers;

[ApiController]
[Route("api/[controller]")]
public class ProductsController : ControllerBase
{
    private readonly StoreContext context;

    public ProductsController(StoreContext context)
    {
        this.context = context;
    }

    [HttpGet] //get all list of products
    public async Task<ActionResult<IEnumerable<Product>>> GetProducts()
    {
        return await context.Products.ToListAsync();
    }

    [HttpGet("{id:int}")] //api/products/2
    public async Task<ActionResult<Product>> GetProducts(int id){
         var product = await  context.Products.FindAsync(id); 
         if(product == null) return NotFound();

         return product;
    }

    //Post Porducts
    [HttpPost]
    public async Task<ActionResult<Product>> CreateProduct(Product product)
    {
        context.Products.Add(product);   

        await context.SaveChangesAsync();

        return product;
    }

    //get product by id
    [HttpPut("{id:int}")]
    public async Task<ActionResult>UpdateProduct(int id, Product product)
    {
        if(product.Id != id || !ProductExists(id)) 
            return BadRequest("Cannot update this product.");

        context.Entry(product).State = EntityState.Modified;

        await context.SaveChangesAsync();

        return NoContent();
    }

    //Delete Product
    [HttpDelete("{id:int}")]
    public async Task<ActionResult> DeleteProduct(int id)
    {
        var product = await context.Products.FindAsync(id);
        if(product == null) return NotFound();

        context.Products.Remove(product);

        await context.SaveChangesAsync();

        return NoContent(); 
    }

    //verify if product is available
    private bool ProductExists(int id)
    {
        return context.Products.Any(x => x.Id == id);
    }
}