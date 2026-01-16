using Microsoft.OpenApi.Models;
using System.Reflection;

namespace Stock.Management.Api.Configuration;

public static class SwaggerConfig
{
    public static IServiceCollection AddSwaggerConfig(this IServiceCollection services)
    {
        services.AddSwaggerGen(c =>
        {
            c.SwaggerDoc("stockmanagement", new OpenApiInfo
            {
                Title = "StockManagement",
                Version = "v1",
                Description = "Serviço para gestão de ações."
            });

            //Configura o caminho para os comentários em XML para o Swagger.
            var xmlFile = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
            var xmlPath = Path.Combine(AppContext.BaseDirectory, xmlFile);
            c.IncludeXmlComments(xmlPath);

        });

        return services;
    }
}