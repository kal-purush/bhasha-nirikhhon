using FoodZone.Data;
using FoodZone.Models.Sercurity;
using Microsoft.AspNetCore.Identity;

namespace FoodZone.API.Configurations
{
    public static class ServiceExtensions
    {
        public static void ConfigureIdentity(this IServiceCollection services)
        {
            var builder = services.AddIdentity<Account, IdentityRole>(q =>
            {
                q.Password.RequireDigit = true;
                q.Password.RequiredLength = 8;
                q.Password.RequireUppercase = true;
                q.Password.RequireLowercase = true;
                q.Password.RequireNonAlphanumeric = false;
            }).AddEntityFrameworkStores<FoodZoneContext>().AddDefaultTokenProviders();
        }
    }
}