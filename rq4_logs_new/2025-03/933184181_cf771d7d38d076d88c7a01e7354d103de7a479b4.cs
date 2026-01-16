
using FluentValidation;
using MainService.Domain.Interfaces;
using MainService.Domain.UseCases;
using MainService.Infras;
using MainService.Infras.Repositories;

public static class DependencyInjectionExtensions
{
    public static IServiceCollection AddProjectServices(this IServiceCollection services)
    {
        // Database Service
        services.AddSingleton<MongoDbService>();

        // Use Cases
        services.AddScoped<UserUseCase>();

        // Repositories
        services.AddScoped<IUserRepository, UserRepository>();
        services.AddScoped<IProjectRepository, projectRepository>();

        return services;
    }

    public static IServiceCollection AddGrpcServices(this IServiceCollection services)
    {
        services.AddGrpc(options =>
        {
            options.Interceptors.Add<GrpcExceptionInterceptor>();
        });
        services.AddGrpcReflection();
        return services;
    }

    public static IServiceCollection AddValidationServices(this IServiceCollection services)
    {
        services.AddValidatorsFromAssemblyContaining<CreateProjectValidator>();
        services.AddValidatorsFromAssemblyContaining<CreateUserValidator>();
        return services;
    }
}