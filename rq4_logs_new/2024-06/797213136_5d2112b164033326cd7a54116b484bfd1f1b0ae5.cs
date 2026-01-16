using DiamondStoreSystem.BusinessLayer.IServices;
using DiamondStoreSystem.BusinessLayer.Services;
using DiamondStoreSystem.Repositories.IRepositories;
using DiamondStoreSystem.Repositories.Repositories;

namespace DiamondStoreSystem.API.AppStart
{
    public static class DependencyInjectionResolver
    {
        public static void ConfigDI(this IServiceCollection services)
        {
            services.AddSingleton(typeof(IBaseRepository<>), typeof(BaseRepository<>));
            services.AddScoped<IVnPaymentService, VnPaymentService>();
            services.AddScoped<IWarrantyService, WarrantyService>();
            services.AddScoped<IAccountService, AccountService>();
            services.AddScoped<IOrderService, OrderService>();
            services.AddScoped<ISubDiamondService, SubDiamondService>();
            services.AddScoped<IProductService, ProductService>();
            services.AddScoped<IAccessoryService, AccessoryService>();
            services.AddScoped<IDiamondService, DiamondService>();
            services.AddScoped<IWarrantyRepository, WarrantyRepository>();
            services.AddScoped<IAccountRepository, AccountRepository>();
            services.AddScoped<IOrderRepository, OrderRepository>();
            services.AddScoped<ISubDiamondRepository, SubDiamondRepository>();
            services.AddScoped<IProductRepository, ProductRepository>();
            services.AddScoped<IAccessoryRepository, AccessoryRepository>();
            services.AddScoped<IDiamondRepository, DiamondRepository>();
            services.AddScoped<IVnPaymentRepository, VnPaymentRepository>();
            services.AddScoped<IAuthService, AuthService>();
        }
    }
}