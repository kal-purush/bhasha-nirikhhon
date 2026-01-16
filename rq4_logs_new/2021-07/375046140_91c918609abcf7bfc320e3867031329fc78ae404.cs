using System;
using System.Linq;
using System.Threading.Tasks;
using Core.Entities.Halko;
using Core.Entities.Identity;
using Core.Interfaces;
using Infrastructure.Data;
using Infrastructure.Identity;
using Microsoft.EntityFrameworkCore;

namespace Api.Extensions
{
    public static class MigrateDatabaseExtension
    {
        public static async Task MigrateHalko(HalkoContext context)
        {
            await context.Database.MigrateAsync();
            await context.Database.ExecuteSqlRawAsync ( "drop table if exists AppUser" );
            await context.Database.ExecuteSqlRawAsync ( "drop table if exists Point" );
            await context.Database.ExecuteSqlRawAsync ( "drop table if exists UserPoints" );
            
        }

        public static async Task MigrateIdentity( AppIdentityDbContext identityContext )
        {
            await identityContext.Database.MigrateAsync();
            await identityContext.Database.ExecuteSqlRawAsync ( "drop table if exists Point" );
            await identityContext.Database.ExecuteSqlRawAsync ( "drop table if exists ParticipantPoint" );
        }
    }
}