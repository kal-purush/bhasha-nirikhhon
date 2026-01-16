using System;
using Integratieproject1.DAL;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Identity.UI;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

[assembly: HostingStartup(typeof(Integratieproject1.Areas.Identity.IdentityHostingStartup))]
namespace Integratieproject1.Areas.Identity
{
    public class IdentityHostingStartup : IHostingStartup
    {
        public void Configure(IWebHostBuilder builder)
        {
            builder.ConfigureServices((context, services) => {
                services.AddDbContext<CityOfIdeasDbContext>(options =>
                    options.UseSqlite("Data Source=CityOfIdeas.db"));

                services.AddDefaultIdentity<IdentityUser>()
                    .AddEntityFrameworkStores<CityOfIdeasDbContext>();
            });
        }
    }
}