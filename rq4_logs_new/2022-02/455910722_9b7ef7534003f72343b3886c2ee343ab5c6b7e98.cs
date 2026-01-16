using Microsoft.AspNetCore.Mvc;
using System.Linq;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Abby.Web.Model;
using Abby.Web.Data;

namespace Abby.Web.Pages.Categories
{
    [BindProperties]
    public class DeleteModel : PageModel
    {
        private readonly ApplicationDbContext _db;
        public Category Category { get; set; }

        public DeleteModel(ApplicationDbContext db)
        {
            _db = db;
        }

       
        public void OnGet(int id)
        {
            Category = _db.Category.Find(id);

        }


        public async Task<IActionResult> OnPost()
        {
            
                var categoryFrromDb = _db.Category.Find(Category.Id);
                if(categoryFrromDb != null)
                {
                    _db.Category.Remove(categoryFrromDb);
                    await _db.SaveChangesAsync();
                TempData["success"] = "Category delete successfully";
                return RedirectToPage("Index");
            }
                
          
            return Page();
        }

    }
}