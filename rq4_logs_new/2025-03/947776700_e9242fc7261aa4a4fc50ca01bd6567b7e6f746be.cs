using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using RazorPage.Models;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Security.Claims;
using System.Threading.Tasks;

namespace RazorPage.Areas.Admin.Pages.Role
{
    [Authorize(Roles = "Admin")]
    public class AddRoleClaimModel : RolePageModel
    {
        public AddRoleClaimModel(RoleManager<IdentityRole> roleManager, MyBlogContext myBlogContext) : base(roleManager, myBlogContext)
        {
        }

        public class InputModel()
        {
            [DisplayName("Tên tính chất")]
            [Required(ErrorMessage ="Vui lòng nhập {0}")]
            [StringLength(256,MinimumLength = 3,ErrorMessage ="{0} phải dài từ {2} cho tới {1} ký tự.")]
            public string ClaimType { get; set; }
            [DisplayName("Giá trị tính chất")]
            [Required(ErrorMessage = "Vui lòng nhập {0}")]
            [StringLength(256, MinimumLength = 3, ErrorMessage = "{0} phải dài từ {2} cho tới {1} ký tự.")]
            public string ClaimValue { get; set; }
        }
        [BindProperty]
        public InputModel Input { get; set; }
        public IdentityRole role { get; set; }

        public async Task<IActionResult> OnGet(string roleid)
        {
            role = await _roleManager.FindByIdAsync(roleid);
            if (role == null)
            {
                return NotFound("Khong tim thay role");
            }
            return Page();
        }
        public async Task<IActionResult> OnPostAsync(string roleid) 
        {
            role = await _roleManager.FindByIdAsync(roleid);
            if (role == null)
            {
                return NotFound("Khong tim thay role");
            }
            
            if (!ModelState.IsValid)
            {
                return Page();
            }

            if((await _roleManager.GetClaimsAsync(role)).Any(c=>c.Type == Input.ClaimType && c.Value == Input.ClaimValue))
            {
                ModelState.AddModelError(string.Empty, "Claims này đã có trong Role");
                return Page();
            }
            var newClaims = new Claim(Input.ClaimType, Input.ClaimValue);
            var result = await _roleManager.AddClaimAsync(role, newClaims);
            if (!result.Succeeded) 
            {
                result.Errors.ToList().ForEach(error =>
                {
                    ModelState.AddModelError(string.Empty, error.Description);
                });
                return Page();
            }
            StatusMessage = "Đã thêm đặc tính thành công";

            return RedirectToPage("./Edit",new {roleid = role.Id});
        }
    }
}