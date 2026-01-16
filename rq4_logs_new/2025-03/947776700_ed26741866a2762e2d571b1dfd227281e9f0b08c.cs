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
    public class EditClaimsModel : RolePageModel
    {
        public EditClaimsModel(RoleManager<IdentityRole> roleManager, MyBlogContext myBlogContext) : base(roleManager, myBlogContext)
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
        public IdentityRoleClaim<string> claims { get; set; }

        public async Task<IActionResult> OnGet(int? claimid)
        {
            if(claimid == null) return NotFound("Khong tim thay role");
            claims = _myBlogContext.RoleClaims.Where(c=> c.Id == claimid).FirstOrDefault();
            if(claims == null) return NotFound("Khong tim thay role");
            role = await _roleManager.FindByIdAsync(claims.RoleId);
            if (role == null)
            {
                return NotFound("Khong tim thay role");
            }

            Input = new InputModel()
            {
                ClaimType = claims.ClaimType,
                ClaimValue = claims.ClaimValue
            };
            return Page();
        }
        public async Task<IActionResult> OnPostAsync(int? claimid) 
        {
            if (claimid == null) return NotFound("Khong tim thay role");
            claims = _myBlogContext.RoleClaims.Where(c => c.Id == claimid).FirstOrDefault();
            if (claims == null) return NotFound("Khong tim thay role");
            role = await _roleManager.FindByIdAsync(claims.RoleId);
            if (role == null)
            {
                return NotFound("Khong tim thay role");
            }

            if (!ModelState.IsValid)
            {
                return Page();
            }

            if(_myBlogContext.RoleClaims.Any(c => c.RoleId== role.Id && c.ClaimType == Input.ClaimType && c.ClaimValue == Input.ClaimValue && c.Id != claims.Id))
            {
                ModelState.AddModelError(string.Empty, "Claims này đã có trong Role");
                return Page();
            }

            claims.ClaimType = Input.ClaimType;
            claims.ClaimValue = Input.ClaimValue;

            await _myBlogContext.SaveChangesAsync();
            StatusMessage = "Đã cập nhật đặc tính thành công";

            return RedirectToPage("./Edit",new {roleid = role.Id});
        }
        public async Task<IActionResult> OnPostDeleteAsync(int? claimid)
        {
            if (claimid == null) return NotFound("Khong tim thay role");
            claims = _myBlogContext.RoleClaims.Where(c => c.Id == claimid).FirstOrDefault();
            if (claims == null) return NotFound("Khong tim thay role");
            role = await _roleManager.FindByIdAsync(claims.RoleId);
            if (role == null)
            {
                return NotFound("Khong tim thay role");
            }

            await _roleManager.RemoveClaimAsync(role, new Claim(claims.ClaimType,claims.ClaimValue));

            await _myBlogContext.SaveChangesAsync();
            StatusMessage = "Đã xóa đặc tính thành công";

            return RedirectToPage("./Edit", new { roleid = role.Id });
        }
    }
}