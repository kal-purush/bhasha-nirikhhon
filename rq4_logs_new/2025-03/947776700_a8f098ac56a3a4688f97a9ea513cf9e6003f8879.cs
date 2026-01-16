using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using RazorPage.Models;
using System.Security.Claims;

namespace RazorPage.Security.Requirements
{
    public class AppAuthorizationHandler : IAuthorizationHandler
    {
        private readonly ILogger<AppAuthorizationHandler> _logger;
        private readonly UserManager<AppUser> _userManager;
        public AppAuthorizationHandler(ILogger<AppAuthorizationHandler> logger, UserManager<AppUser> userManager)
        {
            _logger = logger;
            _userManager = userManager;
        }
        public Task HandleAsync(AuthorizationHandlerContext context)
        {
            
            var requirements = context.PendingRequirements.ToList();
            foreach(var requirement in requirements)
            {
                if(requirement is GenZRequirement)
                {
                    if(IsGenz(context.User,(GenZRequirement)requirement))
                    {
                        context.Succeed(requirement);
                    }
                }
                //if (requirement is OtherRequirement)
                //{
                //xử lý các requirement khác
                //}
                if (requirement is ArticleUpdateRequirement)
                {
                    bool canUpdate = (CanUpdateArticle(context.User, context.Resource, (ArticleUpdateRequirement)requirement));
                    if(canUpdate) context.Succeed(requirement);
                }
            }
            return Task.CompletedTask;
        }

        private bool CanUpdateArticle(ClaimsPrincipal user, object? resource, ArticleUpdateRequirement requirement)
        {
            if (user.IsInRole("Admin"))
            {
                _logger.LogInformation("Admin cap nhat...");
                return true;
            }
            var article = resource as Article;
            var dateCreate = article.Created;
            var dateCanUpdate = new DateTime(requirement.Year, requirement.Month, requirement.Day);
            if(dateCreate < dateCanUpdate)
            {
                _logger.LogInformation("Qua ngay cap nhat");
                return false;
            }
            return true;
        }

        private bool IsGenz(ClaimsPrincipal user, GenZRequirement requirement)
        {
            var appUserTask = _userManager.GetUserAsync(user);
            Task.WaitAll(appUserTask);
            var appUser = appUserTask.Result;

            if(appUser == null)
            {
                _logger.LogInformation($"{appUser.UserName} Khong co ngay sinh. Khong thoa man dieu kien");
                return false;
            }

            int year = appUser.DoB.Value.Year;
            var success = (year >= requirement.FromYear && year <= requirement.ToYear);
            if(success) {
                _logger.LogInformation($"{appUser.UserName} Thoa man dieu kien");
            }
            else
            {
                _logger.LogInformation($"{appUser.UserName} Khong thoa man dieu kien");
            }
                return success;
        }
    }
}