using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using CourseGenerator.DAL.Context;

namespace CourseGenerator.Api.Extensions
{
    public static class HostExtensions
    {
        public static IHost InitDatabase(this IHost host)
        {
            using (var scope = host.Services.CreateScope())
            {
                var services = scope.ServiceProvider;
                ApplicationContext db = services.GetRequiredService<ApplicationContext>();
                
                db.Database.EnsureDeleted();
                db.Database.EnsureCreated();
            }

            return host;
        }
    }
}