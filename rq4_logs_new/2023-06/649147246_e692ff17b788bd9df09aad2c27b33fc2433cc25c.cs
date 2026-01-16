using Microsoft.Extensions.DependencyInjection;
using Cargo.Data.Application.Interfaces;
using Cargo.Data.Application.Interfaces.DataHandlers;
using Cargo.Data.Application.Services;
using Cargo.Data.Core.Interfaces;
using Cargo.Data.Core.Models.Base;
using Cargo.Data.Core.Models;
using Cargo.Data.Core.Services;
using Cargo.Data.Core.Factories;

namespace Cargo.Data.Application;

public static class ConfigureServices
{
    public static void RegisterApplicationServices(this IServiceCollection services)
    {
        services.AddScoped<IEntityTransformationService<Partner>, PartnerTransformationService>();
        services.AddScoped<IEntityTransformationService<Device>, DeviceTransformationService>();
        services.AddScoped<IEntityTransformationService<BaseSensor>, SensorTransformationService>();
        services.AddScoped<IEntityTransformationService<Measurement>, MeasurementTransformationService>();
        services.AddScoped<ISensorFactory, SensorFactory>();

        services.AddScoped<IDataLoaderService, DataLoaderService>();
        services.AddScoped<IDataTransformationService, DataTransformationService>();
    }
}