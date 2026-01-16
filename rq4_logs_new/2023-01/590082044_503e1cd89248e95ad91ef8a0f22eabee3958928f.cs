using CraftHouse.Web.Data;
using CraftHouse.Web.Entities;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace CraftHouse.Web.Pages;

public class Seed : PageModel
{
    private readonly AppDbContext _context;

    public Seed(AppDbContext context)
    {
        _context = context;
    } 
    
    public async Task OnGetAsync()
    {
        var categories = new List<Category>()
        {
            new() { Name = "Test" },
            new() { Name = "Test2" }
        };
        
        _context.Categories.AddRange(categories);

        var products = new List<Product>
        {
            new() { CategoryId = 1, Name = "TestProduct", Price = 10.99f, Description = "Test product" },
            new() { CategoryId = 1, Name = "TestProduct", Price = 10.99f, Description = "Test product" },
            new() { CategoryId = 1, Name = "TestProduct", Price = 10.99f, Description = "Test product" },
            new() { CategoryId = 1, Name = "TestProduct", Price = 10.99f, Description = "Test product" },
            new() { CategoryId = 1, Name = "TestProduct", Price = 10.99f, Description = "Test product" },
            new() { CategoryId = 2, Name = "TestProduct", Price = 10.99f, Description = "Test product" },
            new() { CategoryId = 2, Name = "TestProduct", Price = 10.99f, Description = "Test product" },
            new() { CategoryId = 2, Name = "TestProduct", Price = 10.99f, Description = "Test product" },
            new() { CategoryId = 2, Name = "TestProduct", Price = 10.99f, Description = "Test product" },
            new() { CategoryId = 2, Name = "TestProduct", Price = 10.99f, Description = "Test product" },
        };
        
        _context.Products.AddRange(products);

        await _context.SaveChangesAsync();
    }
}