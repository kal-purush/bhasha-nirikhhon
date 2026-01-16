using Bookworm_Society_API.Data;
using Microsoft.EntityFrameworkCore;

namespace Bookworm_Society_API.Utility
{
    public static class DataHelper
    {
        public static async Task ManageDataAsync(IServiceProvider svcProvider)
        {
            // Service: An instance of db context
            var dbContextSvc = svcProvider.GetRequiredService<Bookworm_SocietyDbContext>();

            // Migration: This is the programmatic equivalent to Update-Database
            await dbContextSvc.Database.MigrateAsync();
        }
    }
}