using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SkillMatrix.DataAccess.Repositories.Languages;
using SkillMatrix.DataAccess.Repositories.Skills;
using SkillMatrix.DataAccess.Repositories.Users;
using SkillMatrix.Domain.Languages.Repositories;
using SkillMatrix.Domain.Skills.Repositories;
using SkillMatrix.Domain.Users.Repositories;


namespace SkillMatrix.DataAccess
{

    public class DataAccessConfig
    {

        public static void ServiceRegistry(IServiceCollection services, IConfiguration configuration)
        {
            services.AddScoped<ISkillRepository, SkillRepository>();
            services.AddScoped<ISkillCategoryRepository, SkillCategoryRepository>();
            services.AddScoped<ILanguageRepository, LanguageRepository>();
            services.AddScoped<IUserRepository, UserRepository>();
            services.AddScoped<ITeamRepository, TeamRepository>();
            services.AddScoped<IDepartmentRepository, DepartmentRepository>();

            var dbSettings = configuration.GetSection("DbSettings").Get<DbSettings>();

            services.AddDbContext<ApplicationDBContext>(options =>
            options.UseSqlServer(dbSettings.ConnectionString)
            .UseLazyLoadingProxies());
        }
    }
}