using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace CartService.Infrastructure.Data
{
    public static class DatabaseInitializer
    {
        public static async Task InitializeHangfireDbAsync(IServiceProvider serviceProvider)
        {
            using var scope = serviceProvider.CreateScope();
            var context = scope.ServiceProvider.GetRequiredService<HangfireDbContext>();

            await context.Database.MigrateAsync();
        }
    }
}