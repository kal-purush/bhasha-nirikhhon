using Microsoft.Extensions.DependencyInjection;
using SPSS.Repository.Repositories.GenericRepository;
using SPSS.Repository.Repositories.ProductRepository;
using SPSS.Repository.UnitOfWork;
using SPSS.Service.Services.AuthService;
using SPSS.Service.Services.FirebaseStorageService;
using SPSS.Service.Services.ProductService;

namespace SPSS
{
    public static class DependencyInjection
    {
        public static IServiceCollection AddApplicationServices(this IServiceCollection services)
        {
            services.AddScoped(typeof(IGenericRepository<>), typeof(GenericRepository<>));

            // UIT
            services.AddScoped<IUnitOfWork, UnitOfWork>();

            // Auth
            services.AddScoped<IAuthService, AuthService>();

            // Product
            services.AddScoped<IProductService, ProductService>();
            services.AddScoped<IProductRepository, ProductRepository>();

            return services;
        }
    }
}