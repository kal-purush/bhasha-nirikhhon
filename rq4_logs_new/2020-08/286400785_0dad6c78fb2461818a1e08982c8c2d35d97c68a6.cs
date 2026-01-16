using Microsoft.AspNetCore.Hosting;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OnlineMarks.Data.Models.Context;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace OnlineMarks.Api
{
    public class DbContextConfig
    {
        public static void Initialize(IConfiguration configuration, IServiceProvider svp)
        {
            var optionsBuilder = new DbContextOptionsBuilder();

            optionsBuilder.UseSqlServer(configuration.GetConnectionString("DefaultConnection"));

            var context = new ApplicationIdentityContext(optionsBuilder.Options);

        }

        public static void Initialize(IServiceCollection services, IConfiguration configuration)

        {
            services.AddDbContext<ApplicationIdentityContext>(options =>
              options.UseSqlServer(configuration.GetConnectionString("DefaultConnection")));
        }

    }
}
