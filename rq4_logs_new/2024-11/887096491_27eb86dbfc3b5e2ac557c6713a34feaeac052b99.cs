using Microsoft.EntityFrameworkCore;
using System.Reflection;

namespace WolfDen.Infrastructure.Extensions
{
    public static class AddConventionsExtension
    {
        public static void AddConventions(this ModelBuilder modelBuilder, string schemaName, Assembly? migrationAssembly)
        {
            modelBuilder.HasDefaultSchema(schemaName);
            if (migrationAssembly != null)
            {
                modelBuilder.ApplyConfigurationsFromAssembly(migrationAssembly);
            }
        }
    }
}