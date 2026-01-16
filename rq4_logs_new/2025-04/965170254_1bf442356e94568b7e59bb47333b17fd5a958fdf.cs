using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.EntityFrameworkCore;
using Vedect.Data;
using Vedect.Models.Domain;
using System.ComponentModel.DataAnnotations;

namespace Vedect.Pages.Admin
{
    public class AssignSubscriptionModel : PageModel
    {
        private readonly AppDbContext _db;
        private readonly ILogger<AssignSubscriptionModel> _logger;

        public AssignSubscriptionModel(AppDbContext db, ILogger<AssignSubscriptionModel> logger)
        {
            _db = db;
            _logger = logger;
        }

        [BindProperty]
        [Required(ErrorMessage = "User is required")]
        public string SelectedUserId { get; set; }

        [BindProperty]
        [Required(ErrorMessage = "Subscription plan is required")]
        public int SelectedPlanId { get; set; }

        public List<User> Users { get; set; } = new();
        public List<SubscriptionPlan> SubscriptionPlans { get; set; } = new();

        public async Task OnGetAsync()
        {
            await LoadDropdownsAsync();
        }

        public async Task<IActionResult> OnPostAsync()
        {
            _logger.LogInformation("OnPostAsync called");
            _logger.LogInformation("SelectedUserId: {SelectedUserId}, SelectedPlanId: {SelectedPlanId}", SelectedUserId, SelectedPlanId);

            if (!ModelState.IsValid)
            {
                _logger.LogWarning("ModelState is invalid.");
                await LoadDropdownsAsync();
                return Page();
            }

            // Add check to see if values are actually populated
            if (string.IsNullOrEmpty(SelectedUserId) || SelectedPlanId == 0)
            {
                _logger.LogWarning("Either User or Plan not selected.");
                ModelState.AddModelError(string.Empty, "Both user and plan must be selected.");
                await LoadDropdownsAsync();
                return Page();
            }

            var user = await _db.Users.FirstOrDefaultAsync(u => u.Id == SelectedUserId);
            if (user == null)
            {
                _logger.LogWarning("User not found.");
                ModelState.AddModelError(string.Empty, "User not found.");
                await LoadDropdownsAsync();
                return Page();
            }

            var plan = await _db.SubscriptionPlans.FirstOrDefaultAsync(p => p.Id == SelectedPlanId);
            if (plan == null)
            {
                _logger.LogWarning("Invalid subscription plan selected.");
                ModelState.AddModelError(string.Empty, "Invalid subscription plan selected.");
                await LoadDropdownsAsync();
                return Page();
            }

            user.SubscriptionPlanId = SelectedPlanId;
            await _db.SaveChangesAsync();

            _logger.LogInformation("Changes saved to the database.");

            TempData["Success"] = "Subscription plan assigned successfully.";
            return RedirectToPage();
        }

        private async Task LoadDropdownsAsync()
        {
            _logger.LogInformation("Loading users and subscription plans...");
            Users = await _db.Users
                .Include(u => u.SubscriptionPlan)
                .AsNoTracking()
                .ToListAsync();

            SubscriptionPlans = await _db.SubscriptionPlans
                .AsNoTracking()
                .ToListAsync();

            _logger.LogInformation("Loaded {UserCount} users and {PlanCount} subscription plans.", Users.Count, SubscriptionPlans.Count);
        }
    }
}