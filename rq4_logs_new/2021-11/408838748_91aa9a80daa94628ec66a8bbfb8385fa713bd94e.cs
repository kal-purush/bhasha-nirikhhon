using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SkillMatrix.DataAccess.Repositories.Languages;
using SkillMatrix.DataAccess.Repositories.Skills;
using SkillMatrix.DataAccess.Repositories.Users;
using SkillMatrix.Domain.Languages.Repositories;
using SkillMatrix.Domain.Skills.Repositories;
using SkillMatrix.Domain.Users.Models;
using SkillMatrix.Domain.Users.Repositories;
using System;

namespace SkillMatrix.DataAccess.Infrastructures
{
    public class DataAccessConfig
    {
        public static void Registry(IServiceCollection services, IConfiguration configuration)
        {
            var connectionStrings = configuration.GetSection("ConnectionStrings");
            services.Configure<ConnectionStrings>(connectionStrings);

            services.AddScoped<ISkillRepository, SkillRepository>();
            services.AddScoped<ISkillCategoryRepository, SkillCategoryRepository>();
            services.AddScoped<ILanguageRepository, LanguageRepository>();
            services.AddScoped<IUserRepository, UserRepository>();
            services.AddScoped<ITeamRepository, TeamRepository>();
            services.AddScoped<IDepartmentRepository, DepartmentRepository>();

            services.AddTransient<ApplicationDBContext>();

            services.AddIdentity<User, IdentityRole>(options =>
            {
                options.User.AllowedUserNameCharacters = string.Empty;
                options.User.RequireUniqueEmail = true;

                // Default Password settings.
                options.Password.RequireDigit = true;
                options.Password.RequireLowercase = true;
                options.Password.RequireNonAlphanumeric = true;
                options.Password.RequireUppercase = true;
                options.Password.RequiredLength = 6;
                options.Password.RequiredUniqueChars = 1;
                options.Password.RequireUppercase = true;
            })
               .AddEntityFrameworkStores<ApplicationDBContext>();
        }

    }
}