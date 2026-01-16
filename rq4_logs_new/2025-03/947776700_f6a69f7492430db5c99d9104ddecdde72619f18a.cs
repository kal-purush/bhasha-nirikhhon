using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using RazorPage.Models;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;

namespace RazorPage.Areas.Admin.Pages.Role
{
    [Authorize(Roles = "Admin")]
    public class CreateModel : RolePageModel
    {

        public CreateModel(RoleManager<IdentityRole> roleManager, MyBlogContext myBlogContext) : base(roleManager, myBlogContext)
        {
        }

        public class InputModel()
        {
            [DisplayName("Tên vai trò")]
            [Required(ErrorMessage ="Vui lòng nhập {0}")]
            [StringLength(256,MinimumLength = 3,ErrorMessage ="{0} phải dài từ {2} cho tới {1} ký tự.")]
            public string Name { get; set; }
        }
        [BindProperty]
        public InputModel Input { get; set; }

        public void OnGet()
        {
        }
        public async Task<IActionResult> OnPostAsync() 
        {
            if (!ModelState.IsValid)
            {
                return Page();
            }

            var role = new IdentityRole(Input.Name);
            var result = await _roleManager.CreateAsync(role);
            if ((result.Succeeded))
            {
                StatusMessage = $"Bạn vừa tạo ra role mới: {Input.Name}";
                return RedirectToPage("./Index");
            }
            else
            {
                result.Errors.ToList().ForEach(error =>
                {
                    ModelState.AddModelError(string.Empty, error.Description);
                });
            }
            return Page();
        }
    }
}