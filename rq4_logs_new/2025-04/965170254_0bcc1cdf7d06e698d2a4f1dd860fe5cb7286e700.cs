using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using System.Security.Cryptography;
using System.Text;
using Vedect.Models.Domain;

namespace Vedect.Pages.Admin
{
    public class LoginModel : PageModel
    {
        private readonly IConfiguration _config;

        public LoginModel(IConfiguration config)
        {
            _config = config;
        }

        [BindProperty] public string Username { get; set; } = "";
        [BindProperty] public string Password { get; set; } = "";
        public string? ErrorMessage { get; set; }

        public IActionResult OnGet()
        {
            if (!string.IsNullOrEmpty(HttpContext.Session.GetString("AdminUsername")))
                return RedirectToPage("/Admin");

            return Page();
        }

        public IActionResult OnPost()
        {
            _ = Request.Form.TryGetValue("Username", out var debugUser);
            _ = Request.Form.TryGetValue("Password", out var debugPass);
            Console.WriteLine($">>> Form Values - Username: {debugUser}, Password: {debugPass}");

            var admins = _config.GetSection("AdminAccounts").Get<List<AdminAccount>>();

            foreach (var admin in admins ?? new List<AdminAccount>())
            {
                Console.WriteLine($">>> Loaded admin from config: {admin.Username} - {admin.PasswordHash}");
            }

            string passwordHash = CalculateSHA256Hash(Password);

            Console.WriteLine($">>> Hashed password from input: {passwordHash}");
             
            var match = admins?.FirstOrDefault(a =>
                a.Username.Equals(Username, StringComparison.OrdinalIgnoreCase)
                && a.PasswordHash == passwordHash);

            if (match is null)
            {
                ErrorMessage = "Invalid credentials";
                return Page();
            }

            HttpContext.Session.SetString("AdminUsername", match.Username);
            return RedirectToPage("/Admin/Index");
        }

        private string CalculateSHA256Hash(string input)
        {
            using var sha256 = SHA256.Create();
            byte[] inputBytes = Encoding.UTF8.GetBytes(input);
            byte[] hashBytes = sha256.ComputeHash(inputBytes);

            return Convert.ToHexString(hashBytes).ToLower(); // good!
        }
    }
}