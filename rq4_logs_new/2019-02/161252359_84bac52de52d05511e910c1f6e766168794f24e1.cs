using System.Web.Http;
using WebActivatorEx;
using Empregando.Relatorios.API;
using Swashbuckle.Application;
using System;

[assembly: PreApplicationStartMethod(typeof(SwaggerConfig), "Register")]

namespace Empregando.Relatorios.API
{
    public class SwaggerConfig
    {
        public static void Register()
        {
            GlobalConfiguration.Configuration
                .EnableSwagger(c =>
                {
                    c.SingleApiVersion("v1", "Empregando.Relatorios.API");
                    c.PrettyPrint();
                    c.UseFullTypeNameInSchemaIds();
                    c.IncludeXmlComments(string.Format(@"{0}\bin\Swagger.XML",
                           AppDomain.CurrentDomain.BaseDirectory));
                })
                .EnableSwaggerUi();
        }
    }
}