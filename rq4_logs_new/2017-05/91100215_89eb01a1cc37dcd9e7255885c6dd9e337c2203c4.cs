using BrewFree.Data;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace BrewFree.Extensions.Startup
{
    public static class SeedDataConfig
    {
        public static void UseSeedData(this IApplicationBuilder app)
        {
            using (var scope = app.ApplicationServices.GetRequiredService<IServiceScopeFactory>().CreateScope())
            {
                var context = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();
                SeedData.EnsureSeedData(context);
            }
        }
    }
}