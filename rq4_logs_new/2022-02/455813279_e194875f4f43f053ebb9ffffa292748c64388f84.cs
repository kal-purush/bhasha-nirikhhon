using Microsoft.Extensions.DependencyInjection;
using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;
using System.Collections.Generic;

namespace OLT.Extensions.SwaggerGen
{
    /// <summary>
    /// Represents the Swagger/Swashbuckle operation filter used to camel case api route parameter. <see href="https://github.com/OuterlimitsTech/olt-dotnet-extensions-swagger/issues/11"/>
    /// </summary>
    public class OltCamelCasingOperationFilter : IOltOperationFilter
    {
        /// <summary>
        /// Applies the filter to the specified operation using the given context.
        /// </summary>
        /// <param name="operation">The operation to apply the filter to.</param>
        /// <param name="context">The current operation filter context.</param>
        public void Apply(OpenApiOperation operation, OperationFilterContext context)
        {
            if (operation.Parameters != null)
            {
                foreach (var item in operation.Parameters)
                {
                    item.Name = ToCamelCase(item.Name);
                }
            }
        }

        public void Apply(SwaggerGenOptions opt)
        {
            opt.OperationFilter<OltCamelCasingOperationFilter>();
        }


        public static string ToCamelCase(string str) => string.IsNullOrEmpty(str) || str.Length < 2 ? str : char.ToLowerInvariant(str[0]) + str.Substring(1);
    }
}