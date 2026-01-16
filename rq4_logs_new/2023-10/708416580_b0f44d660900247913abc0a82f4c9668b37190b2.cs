using Microsoft.AspNetCore.Mvc;
using BookWebEcommerce.Models;
using BookWebEcommerce.Data;

namespace BookWebEcommerce.ViewComponents
{
    public class AuthorViewComponent : ViewComponent
    {
        AppDbContext _context;
        List<Author> authorList;

        public AuthorViewComponent(AppDbContext context)
        {
            _context = context;
            authorList = _context.Authors.ToList();
        }
        public async Task<IViewComponentResult> InvokeAsync()
        {
            return View(authorList);
        } 
    }
}