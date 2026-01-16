using System.Formats.Asn1;
using CraftHouse.Web.Data;
using CraftHouse.Web.Entities;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace CraftHouse.Web.Pages;

public class CategoryAdd : PageModel
{
    private readonly AppDbContext _context;

    public CategoryAdd(AppDbContext context)
    {
        _context = context;
    }

    [BindProperty]
    public string Name { get; set; } = null!;

    public void OnGet()
    {
        
    }

    public async Task OnPostAsync()
    {
        var category = new Category()
        {
            Name = Name
        };
        
        await _context.Categories.AddAsync(category);
        await _context.SaveChangesAsync();
    }
}